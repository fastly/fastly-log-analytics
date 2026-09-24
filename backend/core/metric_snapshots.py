"""Minute-cadence operational-vital snapshots.

Backs the trend lines on the admin System Health card / Trends tab. Each
row is one numeric sample: ``(ts, metric, service_id?, task?, value)``.
All snapshots are stored in PostgreSQL standard mode in the ``metric_snapshots``
table.

Public surface
--------------
- :func:`record_snapshot` — sampler writes one row per metric per tick.
- :func:`get_history` — read a single time-series (one ``metric`` /
  ``service_id`` / ``task``); the symmetric single-series counterpart to
  :func:`get_batch`.
- :func:`get_batch` — admin endpoint reads many series in one call.
- :func:`purge_old` — daily cleanup cron drops rows past retention.
- :func:`teardown` / :func:`close_all_connections` — test helpers.
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import iso_z, iso_z_now, parse_relative_time_window

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join("data", "system")
_DB_NAME = "system_metrics.db"
_EMPTY = ""

# Module-level state retained for compatibility with test fixtures.
_local = threading.local()
_init_lock = threading.Lock()
_initialized = False


def _use_postgres() -> bool:
    """Always True in Postgres-only mode."""
    return True


def _pg_connection():
    from backend.core.metadata.pg_connection import get_pg_readonly_connection

    return get_pg_readonly_connection()


def _pg_write_connection():
    from backend.core.metadata.pg_connection import get_pg_thread_connection

    return get_pg_thread_connection()


def _db_path() -> str:
    return os.path.join(_DATA_DIR, _DB_NAME)


def get_db_path() -> str:
    return _db_path()


# ── Write path ───────────────────────────────────────────────────────────────


def record_snapshot(
    metric: str,
    value: float,
    *,
    service_id: str | None = None,
    task: str | None = None,
    ts: str | None = None,
) -> None:
    """Insert one metric sample. Idempotent on ``(metric, service_id, task, ts)``."""
    if not metric:
        raise ValueError("metric is required")
    ts_str = ts or iso_z_now()
    try:
        con = _pg_write_connection()
        con.execute(
            """
            INSERT INTO metric_snapshots (ts, metric, service_id, task, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (metric, service_id, task, ts)
            DO UPDATE SET value = EXCLUDED.value
            """,
            (ts_str, metric, service_id or _EMPTY, task or _EMPTY, float(value)),
        )
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres insert failed for %s: %s", metric, e)


# ── Read path ────────────────────────────────────────────────────────────────


def get_history(
    metric: str,
    *,
    since: datetime | str,
    service_id: str | None = None,
    task: str | None = None,
) -> list[dict]:
    """Return rows newer than ``since`` sorted oldest→newest.

    Each row: ``{"ts": "...", "value": 12.3}``. Service / task are baked
    into the query when provided so the caller doesn't have to filter.
    """
    if isinstance(since, str):
        since = parse_relative_time_window(since)
    try:
        with closing(_pg_connection()) as con:
            rows = con.execute(
                """
                SELECT ts, value FROM metric_snapshots
                WHERE metric = %s AND service_id = %s AND task = %s AND ts >= %s
                ORDER BY ts ASC
                """,
                (metric, service_id or _EMPTY, task or _EMPTY, iso_z(since)),
            ).fetchall()
        return [{"ts": r["ts"], "value": r["value"]} for r in rows]
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres history read failed: %s", e)
        return []


def get_batch(*, since: datetime | str) -> dict:
    """Return every series newer than ``since``, grouped by series key.

    Series key shape: ``"{metric}"`` for global, ``"{metric}|{service_id}"``
    for per-service, ``"{metric}|{service_id}|{task}"`` for per-task. The
    admin Trends page does one round-trip; the frontend partitions
    by metric prefix.
    """
    if isinstance(since, str):
        since = parse_relative_time_window(since)
    try:
        with closing(_pg_connection()) as con:
            rows = con.execute(
                """
                SELECT metric, service_id, task, ts, value
                FROM metric_snapshots
                WHERE ts >= %s
                ORDER BY metric, service_id, task, ts ASC
                """,
                (iso_z(since),),
            ).fetchall()
        return _group_batch_rows(rows)
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres batch read failed: %s", e)
        return {}


# ── Liveness ─────────────────────────────────────────────────────────────────


def last_snapshot_age_s() -> float | None:
    """Seconds since the most recent metric_snapshots row, or ``None`` if empty.

    SRE-06: the minute-cadence sampler (:mod:`backend.cron.jobs.metric_snapshot`)
    is a *global* APScheduler interval job, so a stale ``max(ts)`` is a direct
    witness that the scheduler thread has stopped ticking.
    """
    try:
        with closing(_pg_connection()) as con:
            row = con.execute("SELECT max(ts) AS latest FROM metric_snapshots").fetchone()
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres liveness read failed: %s", e)
        return None
    latest = row["latest"] if row else None
    if not latest:
        return None
    from backend.utils.date_utils import parse_iso_utc

    dt = parse_iso_utc(latest)
    if dt is None:
        return None
    return max(0.0, (datetime.now(UTC) - dt).total_seconds())


# ── Retention ────────────────────────────────────────────────────────────────


def purge_old(retention_days: int = 30) -> int:
    """Delete rows older than ``retention_days``. Returns the row count."""
    if retention_days <= 0:
        return 0
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    try:
        con = _pg_write_connection()
        cur = con.execute("DELETE FROM metric_snapshots WHERE ts < %s", (cutoff,))
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres purge failed: %s", e)
        return 0


# ── Test / teardown helpers ──────────────────────────────────────────────────


def close_all_connections() -> None:
    """Close any thread-local connection (no-op under Postgres)."""
    pass


def _group_batch_rows(rows) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in rows:
        key = r["metric"]
        svc = r["service_id"]
        if svc:
            key = f"{key}|{svc}"
            task = r["task"]
            if task:
                key = f"{key}|{task}"
        out.setdefault(key, []).append({"ts": r["ts"], "value": r["value"]})
    return out


def teardown() -> None:
    """Clean up snapshots for test isolation."""
    try:
        con = _pg_write_connection()
        con.execute("TRUNCATE TABLE metric_snapshots")
    except Exception:
        pass
