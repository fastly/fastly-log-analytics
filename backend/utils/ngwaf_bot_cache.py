"""SQLite cache for NGWAF verified-bot requests.

WAL mode lets multiple sqlite3 writers (per-service ``_run_ngwaf_bot_sync``
ticks) touch this file concurrently without blocking each other.

Readers (``backend.repositories._base.ensure_ngwaf_bots_materialized`` and
``backend.core.rollups.ngwaf_bots``) deliberately do NOT let DuckDB open
this file directly (no ``ATTACH ... TYPE SQLITE``, no ``sqlite_scan``) —
DuckDB's SQLite reader doesn't reliably coordinate with SQLite's own
WAL/locking protocol, and reading this globally-shared, frequently-written
file that way corrupted it in production (2026-07-30). They read it via
plain ``sqlite3`` instead and hand DuckDB the already-fetched rows.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from backend.utils.date_utils import iso_z, iso_z_now, parse_iso_utc

_CACHE_DIR = Path("data")
_DB_NAME = "ngwaf_bot_cache.db"

_DDL = """
CREATE TABLE IF NOT EXISTS ngwaf_bots (
    waf_req_id         TEXT PRIMARY KEY,
    bot_name           TEXT,
    category           TEXT,
    wellknown_bot_id   TEXT,
    wellknown_bot_name TEXT,
    synced_at          TEXT
);

CREATE TABLE IF NOT EXISTS ngwaf_sync_state (
    workspace_id          TEXT PRIMARY KEY,
    last_timestamp_synced TEXT
);
"""


def _db_path() -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR / _DB_NAME


def get_db_path() -> str:
    return str(_db_path())


def _get_conn() -> sqlite3.Connection:
    from backend.core.sqlite_pool import open_small_cache_db

    return open_small_cache_db(_db_path(), ddl=_DDL)


def ensure_schema() -> None:
    """Create the cache file and tables if they don't exist yet.

    Callers that only *read* the cache (e.g. the sync planner that decides
    whether to fetch new bot data) need the tables to exist before attaching,
    even if no bot has been written yet. This is a chicken-and-egg fix: without
    it, the planner sees an empty file, the LEFT JOIN throws, the planner
    returns None, and the sync exits before upsert_bots ever runs to create
    the tables.
    """
    con = _get_conn()
    con.close()


def get_last_timestamp(workspace_id: str) -> str | None:
    """Return last_timestamp_synced for workspace, or None if no sync has run yet."""
    con = _get_conn()
    try:
        row = con.execute(
            "SELECT last_timestamp_synced FROM ngwaf_sync_state WHERE workspace_id = ?",
            (workspace_id,),
        ).fetchone()
        if row and row[0]:
            return row[0]
        return None
    finally:
        con.close()


def update_sync_watermark(workspace_id: str, until_ts: str) -> None:
    """Advance the high-water mark to until_ts after a completed scan.

    Called at the end of every successful (non-budget-exceeded) sync so the
    next run starts from the end of the last scan instead of rescanning from
    oldest_unenriched_timestamp forever.
    """
    con = _get_conn()
    try:
        with con:
            con.execute(
                "INSERT OR REPLACE INTO ngwaf_sync_state (workspace_id, last_timestamp_synced) VALUES (?, ?)",
                (workspace_id, until_ts),
            )
    finally:
        con.close()


def upsert_bots(records: list[dict], workspace_id: str, latest_timestamp: str | None) -> None:
    """Insert or replace bot records and update sync state in one transaction. Idempotent."""
    con = _get_conn()
    now = iso_z_now()
    rows = [
        (
            r["waf_req_id"],
            r.get("bot_name"),
            r.get("category"),
            r.get("wellknown_bot_id"),
            r.get("wellknown_bot_name"),
            now,
        )
        for r in records
        if r.get("waf_req_id")
    ]
    try:
        with con:
            if rows:
                con.executemany(
                    """
                    INSERT OR REPLACE INTO ngwaf_bots
                        (waf_req_id, bot_name, category, wellknown_bot_id, wellknown_bot_name, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            if latest_timestamp:
                # Advance by 1 second so the next sync uses an exclusive lower bound
                # and doesn't re-fetch the last event we already stored.
                try:
                    _pts = parse_iso_utc(latest_timestamp)
                    next_ts = iso_z(_pts + timedelta(seconds=1)) if _pts else latest_timestamp
                except ValueError:
                    next_ts = latest_timestamp
                con.execute(
                    "INSERT OR REPLACE INTO ngwaf_sync_state (workspace_id, last_timestamp_synced) VALUES (?, ?)",
                    (workspace_id, next_ts),
                )
    finally:
        con.close()


def cleanup_old_bots(retention_days: int) -> int:
    """Delete rows with synced_at older than retention_days. Returns deleted row count."""
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    con = _get_conn()
    try:
        with con:
            cur = con.execute("DELETE FROM ngwaf_bots WHERE synced_at < ?", (cutoff,))
            return cur.rowcount
    finally:
        con.close()
