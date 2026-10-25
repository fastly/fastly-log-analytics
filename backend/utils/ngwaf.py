"""Fastly NGWAF API client — fetch verified-bot requests for a workspace."""

from __future__ import annotations

import logging
import math
from collections.abc import Generator
from datetime import UTC, datetime, timedelta

from backend.core.fastly.client import fastly_paginate
from backend.core.metadata.pg_connection import (
    get_pg_readonly_connection,
    get_pg_thread_connection,
)
from backend.utils.date_utils import iso_z, iso_z_now, parse_iso_utc

logger = logging.getLogger(__name__)

# NGWAF requests live under the shared Fastly API host; fastly() prepends
# https://api.fastly.com, so paths here start with the /ngwaf/v1 prefix.
_API_PREFIX = "/ngwaf/v1"


def fetch_verified_bots_paged(
    api_key: str,
    workspace_id: str,
    from_ts: str,
    until_ts: str | None = None,
    page_limit: int = 500,
) -> Generator[tuple[list[dict], str | None, int]]:
    """Yield (page_records, latest_timestamp, raw_page_count) for each page of VERIFIED-BOT requests.

    page_records contains dicts with keys:
        waf_req_id, bot_name, category, user_agent, server_name, timestamp

    latest_timestamp is the max timestamp seen in the page, or None if the page
    was empty. Callers should persist this after each page so a crash mid-pagination
    doesn't lose progress.

    raw_page_count is the total number of records returned by the API on this page
    before client-side VERIFIED-BOT filtering. Useful for diagnosing filter issues.

    Pagination stops when meta.next_cursor is "" (empty string, not null).
    """
    from backend.core.fastly.client import is_mock_mode

    if is_mock_mode():
        yield [], None, 0
        return

    # Round up the 'from' range to a larger interval (e.g. -5d, -15min) as requested
    # to ensure we don't miss records near the edge due to clock drift or pipeline lag.
    relative_from = _get_relative_time_range(from_ts)

    q = f"tag:VERIFIED-BOT from:{relative_from}"
    if until_ts:
        relative_until = _get_relative_time_range(until_ts)
        q += f" until:{relative_until}"

    for data in fastly_paginate(
        f"{_API_PREFIX}/workspaces/{workspace_id}/requests",
        token=api_key,
        style="cursor",
        params={"q": q, "limit": page_limit},
        page_delay=0.1,  # courtesy pause: no rate-limit headers on this API
    ):
        raw_data = data.get("data", [])
        records: list[dict] = []
        latest_ts: str | None = None

        for r in raw_data:
            record = _extract_bot_record(r)
            if record:
                records.append(record)
                ts = record.get("timestamp")
                if ts and (latest_ts is None or ts > latest_ts):
                    latest_ts = ts

        yield records, latest_ts, len(raw_data)


def _get_relative_time_range(ts_str: str) -> str:
    """Convert an ISO timestamp to a rounded-up relative NGWAF time string (e.g. -5d, -15min)."""
    ts = parse_iso_utc(ts_str)
    if ts is None:
        # If it's already relative or invalid format, just return it
        if ts_str.startswith("-"):
            return ts_str
        return "-1d"  # Safe default for sync

    delta = datetime.now(UTC) - ts
    seconds = delta.total_seconds()

    if seconds > 86400:  # > 1 day
        days = math.ceil(seconds / 86400)
        return f"-{days}d"

    if seconds > 3600:  # > 1 hour
        hours = math.ceil(seconds / 3600)
        return f"-{hours}h"

    # Minutes: round up to next 15 min increment
    mins = math.ceil(seconds / 900) * 15
    return f"-{max(mins, 1)}min"


def _extract_bot_record(r: dict) -> dict | None:
    """Extract bot info from a single NGWAF request record. Returns None if no VERIFIED-BOT signal."""
    signals = r.get("signals", [])

    verified = next((s for s in signals if s.get("id") == "VERIFIED-BOT"), None)
    if not verified:
        return None

    subcat = next(
        (s for s in signals if s.get("id", "").startswith("VERIFIED-BOT.") and s["id"] != "VERIFIED-BOT"),
        None,
    )

    bot_name = verified.get("value")
    category = subcat["id"].replace("VERIFIED-BOT.", "") if subcat else None

    # UA: prefer top-level field, fall back to request_headers list of {name, value} dicts
    user_agent = r.get("user_agent") or next(
        (
            h["value"]
            for h in r.get("request_headers", [])
            if isinstance(h, dict) and h.get("name", "").lower() == "user-agent"
        ),
        None,
    )

    return {
        "waf_req_id": r.get("id"),
        "bot_name": bot_name,
        "category": category,
        "user_agent": user_agent,
        "server_name": r.get("server_name"),
        "timestamp": r.get("timestamp"),
    }


def oldest_unenriched_timestamp(src: dict) -> str | None:
    """Return the oldest log-table timestamp that has a waf_req_id not yet in
    the bot cache. Returns None if the query fails or the log table has no
    unenriched rows.

    Using the log table as the source of truth means we never skip historical
    records — we sync exactly as far back as the data that needs enrichment.
    """
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    table_name = _safe_table(src["name"])

    try:
        # read_only: ATTACH + SELECT only, no writes.
        con = get_connection(source=src, max_wait=5, skip_view_update=True, read_only=True)
        try:
            from backend.repositories._base import attach_ngwaf_cache

            # We don't have actual_cols handy, but we know waf_req_id is required
            # so we pass it explicitly to force the attachment.
            with attach_ngwaf_cache(con, ["waf_req_id"], alias="_ngwaf_oldest") as attached:
                if attached:
                    row = con.execute(f"""
                        SELECT MIN(t.timestamp)
                        FROM {table_name} t
                        LEFT JOIN temp._ngwaf_oldest_ngwaf_bots nb USING (waf_req_id)
                        WHERE t.waf_req_id IS NOT NULL
                          AND t.waf_req_id != ''
                          AND t.waf_sig LIKE '%VERIFIED-BOT%'
                          AND nb.waf_req_id IS NULL
                    """).fetchone()
                    ts = row[0] if row and row[0] else None
                    if ts:
                        # DuckDB returns a datetime object for timestamp columns
                        if hasattr(ts, "strftime"):
                            return iso_z(ts)
                        return str(ts)
        finally:
            con.close()
    except Exception:
        logger.debug("oldest_unenriched_timestamp: con.close failed (ignored)", exc_info=True)

    return None


def _get_conn():
    return get_pg_thread_connection()


def _get_readonly_conn():
    return get_pg_readonly_connection()


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
