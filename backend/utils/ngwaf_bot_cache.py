"""NGWAF verified-bot requests cache against PostgreSQL.

In PostgreSQL standard mode, ngwaf_bots and ngwaf_sync_state live in the
PostgreSQL metadata database.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.core.metadata.pg_connection import (
    get_pg_readonly_connection,
    get_pg_thread_connection,
)
from backend.utils.date_utils import iso_z, iso_z_now, parse_iso_utc


def _get_conn():
    return get_pg_thread_connection()


def _get_readonly_conn():
    return get_pg_readonly_connection()


def ensure_schema() -> None:
    """No-op under Postgres — schema is bootstrapped by pg_schema.py."""
    pass


def _db_path() -> str:
    """Compatibility shim for path resolution probes."""
    import os
    from backend import config
    return os.path.join(config.DATA_DIR, "ngwaf_bot_cache.db")


def get_last_timestamp(workspace_id: str) -> str | None:
    """Return last_timestamp_synced for workspace, or None if no sync has run yet."""
    con = _get_readonly_conn()
    try:
        row = con.execute(
            "SELECT last_timestamp_synced FROM ngwaf_sync_state WHERE workspace_id = ?",
            (workspace_id,),
        ).fetchone()
        if row and row["last_timestamp_synced"]:
            return row["last_timestamp_synced"]
        return None
    finally:
        con.close()


def update_sync_watermark(workspace_id: str, until_ts: str) -> None:
    """Advance the high-water mark to until_ts after a completed scan."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO ngwaf_sync_state (workspace_id, last_timestamp_synced)
            VALUES (?, ?)
            ON CONFLICT (workspace_id) DO UPDATE SET last_timestamp_synced = EXCLUDED.last_timestamp_synced
            """,
            (workspace_id, until_ts),
        )
        con.commit()
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
        if rows:
            con.executemany(
                """
                INSERT INTO ngwaf_bots
                    (waf_req_id, bot_name, category, wellknown_bot_id, wellknown_bot_name, synced_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (waf_req_id) DO UPDATE SET
                    bot_name = EXCLUDED.bot_name,
                    category = EXCLUDED.category,
                    wellknown_bot_id = EXCLUDED.wellknown_bot_id,
                    wellknown_bot_name = EXCLUDED.wellknown_bot_name,
                    synced_at = EXCLUDED.synced_at
                """,
                rows,
            )
        if latest_timestamp:
            try:
                _pts = parse_iso_utc(latest_timestamp)
                next_ts = iso_z(_pts + timedelta(seconds=1)) if _pts else latest_timestamp
            except ValueError:
                next_ts = latest_timestamp
            con.execute(
                """
                INSERT INTO ngwaf_sync_state (workspace_id, last_timestamp_synced)
                VALUES (?, ?)
                ON CONFLICT (workspace_id) DO UPDATE SET last_timestamp_synced = EXCLUDED.last_timestamp_synced
                """,
                (workspace_id, next_ts),
            )
        con.commit()
    finally:
        con.close()


def cleanup_old_bots(retention_days: int) -> int:
    """Delete rows with synced_at older than retention_days. Returns deleted row count."""
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    con = _get_conn()
    try:
        cur = con.execute("DELETE FROM ngwaf_bots WHERE synced_at < ?", (cutoff,))
        con.commit()
        return cur.rowcount or 0
    finally:
        con.close()


def get_cache_stats() -> dict:
    """Return summary statistics of cached NGWAF bot records."""
    con = _get_readonly_conn()
    try:
        cur = con.execute("SELECT count(*) FROM ngwaf_bots")
        total_bots = cur.fetchone()[0]

        workspaces: dict[str, str | None] = {}
        cur = con.execute("SELECT workspace_id, last_timestamp_synced FROM ngwaf_sync_state")
        for r in cur.fetchall():
            workspaces[r["workspace_id"]] = r["last_timestamp_synced"]

        cur = con.execute(
            "SELECT bot_name, count(*) AS cnt FROM ngwaf_bots WHERE bot_name IS NOT NULL GROUP BY bot_name ORDER BY count(*) DESC LIMIT 10"
        )
        top_bots = {r["bot_name"]: r["cnt"] for r in cur.fetchall()}

        return {
            "total_cached_bots": total_bots,
            "workspaces": workspaces,
            "top_bots": top_bots,
        }
    finally:
        con.close()
