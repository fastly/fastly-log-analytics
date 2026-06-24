"""Minute-cadence operational-vital snapshots.

Backs the trend lines on the admin System Health card / Trends tab. Each
row is one numeric sample: ``(ts, metric, service_id?, task?, value)``.

Singleton SQLite file under ``data/system/system_metrics.db`` — the
same convention as ``remote_share.db``. One writer (the sampler cron
job), many readers (the admin endpoint). WAL pragmas mirror the other
singleton caches (ngwaf_bot_cache, remote_share).

Public surface
--------------
- :func:`record_snapshot` — sampler writes one row per metric per tick.
- :func:`get_history` — read a single time-series (one ``metric`` /
  ``service_id`` / ``task``); the symmetric single-series counterpart to
  :func:`get_batch`.
- :func:`get_batch` — admin endpoint reads many series in one call.
- :func:`purge_old` — daily cleanup cron drops rows past retention.
- :func:`teardown` / :func:`close_all_connections` — pytest fixtures.

Why a new singleton file (vs. per-service ``metadata.db``):
- Global metrics (CPU, mem, disk) don't have a natural service scope.
- One writer means no per-service WAL fragmentation under load.
- Keeps per-service migration histories clean (no add-then-revert risk).
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join("data", "system")
_DB_NAME = "system_metrics.db"

_DDL = """
CREATE TABLE IF NOT EXISTS metric_snapshots (
    ts          TEXT NOT NULL,
    metric      TEXT NOT NULL,
    service_id  TEXT NOT NULL DEFAULT '',
    task        TEXT NOT NULL DEFAULT '',
    value       REAL NOT NULL,
    PRIMARY KEY (metric, service_id, task, ts)
);
CREATE INDEX IF NOT EXISTS idx_metric_lookup ON metric_snapshots (metric, ts);
"""

# service_id and task use empty-string sentinels rather than NULL because
# SQLite treats every NULL as distinct under composite PK uniqueness — so a
# replay of the same (metric, NULL, NULL, ts) tuple would NOT collide and
# the sampler would silently double-stamp on a same-second retry. Empty
# strings collide cleanly so INSERT OR REPLACE actually dedupes.
_EMPTY = ""

_local = threading.local()
_init_lock = threading.Lock()
_initialized = False


def _db_path() -> str:
    os.makedirs(_DATA_DIR, exist_ok=True)
    return os.path.join(_DATA_DIR, _DB_NAME)


def get_db_path() -> str:
    return _db_path()


def _init(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute("PRAGMA cache_size=-8000")  # 8 MB — tiny table
    con.executescript(_DDL)
    con.commit()


def _get_con() -> sqlite3.Connection:
    """Thread-local read-write connection.

    Lazily creates the file + applies the schema on the first call per
    process. Subsequent calls reuse the connection (sqlite3 connections
    are not safe to share across threads, so we cache per-thread).
    """
    global _initialized
    con = getattr(_local, "con", None)
    if con is not None:
        return con
    with _init_lock:
        con = sqlite3.connect(_db_path(), timeout=10, check_same_thread=False)
        con.row_factory = sqlite3.Row
        if not _initialized:
            _init(con)
            _initialized = True
        _local.con = con
        return con


def _open_readonly() -> sqlite3.Connection:
    """Short-lived read-only connection.

    URI ``mode=ro`` guarantees the open cannot acquire the writer lock,
    so a slow paginated read can never block the sampler. Raises
    ``OperationalError`` if the file doesn't exist yet — callers should
    treat that as "no samples yet" and return an empty result.
    """
    uri = f"file:{_db_path()}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    return con


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
    con = _get_con()
    try:
        con.execute(
            "INSERT OR REPLACE INTO metric_snapshots (ts, metric, service_id, task, value) VALUES (?, ?, ?, ?, ?)",
            (ts_str, metric, service_id or _EMPTY, task or _EMPTY, float(value)),
        )
        con.commit()
    except Exception as e:
        logger.warning("[metric_snapshots] insert failed for %s: %s", metric, e)


# ── Read path ────────────────────────────────────────────────────────────────


def get_history(
    metric: str,
    *,
    since: datetime,
    service_id: str | None = None,
    task: str | None = None,
) -> list[dict]:
    """Return rows newer than ``since`` sorted oldest→newest.

    Each row: ``{"ts": "...", "value": 12.3}``. Service / task are baked
    into the query when provided so the caller doesn't have to filter.
    """
    try:
        con = _open_readonly()
    except sqlite3.OperationalError:
        return []
    try:
        cutoff = iso_z(since)
        rows = con.execute(
            "SELECT ts, value FROM metric_snapshots "
            "WHERE metric = ? AND service_id = ? AND task = ? AND ts >= ? "
            "ORDER BY ts ASC",
            (metric, service_id or _EMPTY, task or _EMPTY, cutoff),
        ).fetchall()
        return [{"ts": r["ts"], "value": r["value"]} for r in rows]
    finally:
        con.close()


def get_batch(*, since: datetime) -> dict:
    """Return every series newer than ``since``, grouped by series key.

    Series key shape: ``"{metric}"`` for global, ``"{metric}|{service_id}"``
    for per-service, ``"{metric}|{service_id}|{task}"`` for per-task. The
    admin Trends page does one round-trip; the frontend partitions
    by metric prefix.
    """
    try:
        con = _open_readonly()
    except sqlite3.OperationalError:
        return {}
    try:
        cutoff = iso_z(since)
        rows = con.execute(
            "SELECT metric, service_id, task, ts, value FROM metric_snapshots "
            "WHERE ts >= ? ORDER BY metric, service_id, task, ts ASC",
            (cutoff,),
        ).fetchall()
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
    finally:
        con.close()


# ── Liveness ─────────────────────────────────────────────────────────────────


def last_snapshot_age_s() -> float | None:
    """Seconds since the most recent metric_snapshots row, or ``None`` if empty.

    SRE-06: the minute-cadence sampler (:mod:`backend.cron.jobs.metric_snapshot`)
    is a *global* APScheduler interval job, so a stale ``max(ts)`` is a direct
    witness that the scheduler thread has stopped ticking — the one signal that
    distinguishes a dead scheduler (``list_active_runs() == []`` AND nothing
    advancing) from a healthy-idle one. Returns ``None`` when no samples exist
    yet (fresh boot) so the caller renders "unknown" rather than a false alarm.

    Doubles as the SRE-21 snapshot-integrity probe: if the sampler dies or its
    writes start failing, this age climbs without bound.
    """
    try:
        con = _open_readonly()
    except sqlite3.OperationalError:
        return None
    try:
        row = con.execute("SELECT max(ts) AS latest FROM metric_snapshots").fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()
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
    con = _get_con()
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    try:
        cur = con.execute("DELETE FROM metric_snapshots WHERE ts < ?", (cutoff,))
        con.commit()
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("[metric_snapshots] purge failed: %s", e)
        return 0


# ── Test / teardown helpers ──────────────────────────────────────────────────


def close_all_connections() -> None:
    """Close any thread-local connection. Used by pytest fixtures."""
    con = getattr(_local, "con", None)
    if con is not None:
        try:
            con.close()
        except Exception:
            pass
        _local.con = None


def teardown() -> None:
    """Close the connection and delete the DB file + WAL/SHM siblings."""
    global _initialized
    close_all_connections()

    from backend.core.sqlite_pool import remove_sqlite_db_files

    remove_sqlite_db_files(_db_path(), name="metric_snapshots")
    _initialized = False
