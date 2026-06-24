"""Fastly NGWAF API client — fetch verified-bot requests for a workspace."""

from __future__ import annotations

import json
import math
import time
import urllib.parse
import urllib.request
from collections.abc import Generator
from datetime import UTC, datetime

from backend.utils.date_utils import iso_z, parse_iso_utc

_API_BASE = "https://api.fastly.com/ngwaf/v1"


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
    # Round up the 'from' range to a larger interval (e.g. -5d, -15min) as requested
    # to ensure we don't miss records near the edge due to clock drift or pipeline lag.
    relative_from = _get_relative_time_range(from_ts)

    q = f"tag:VERIFIED-BOT from:{relative_from}"
    if until_ts:
        relative_until = _get_relative_time_range(until_ts)
        q += f" until:{relative_until}"

    params: dict[str, str] = {
        "q": q,
        "limit": str(page_limit),
    }

    cursor: str = "initial"  # Non-empty sentinel to enter the loop

    # R-3b: short-circuit on FASTLY_MOCK_MODE so the Playwright E2E + the
    # contract suite don't need a real NGWAF workspace. Production never
    # sets the env var; the gate is a no-op outside the test harness.
    from backend.core.fastly.mock_fixtures import is_mock_mode, mock_ngwaf_verified_bots_page

    if is_mock_mode():
        payload = mock_ngwaf_verified_bots_page()
        yield ([], None, len(payload.get("data", [])))
        return

    while cursor:
        query_string = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items())
        url = f"{_API_BASE}/workspaces/{workspace_id}/requests?{query_string}"
        req = urllib.request.Request(
            url,
            headers={
                "Fastly-Key": api_key,
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

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

        # Empty string means no more pages — stop. None would also stop but the
        # API returns "" not null when exhausted, so `while cursor:` is correct.
        cursor = data.get("meta", {}).get("next_cursor", "")
        if cursor:
            params["cursor"] = cursor
            time.sleep(0.1)  # 100ms inter-page courtesy sleep (no rate-limit headers)
        else:
            # Remove cursor param so it doesn't pollute next loop guard check
            params.pop("cursor", None)


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
    import os

    from backend import config as svcconfig
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table
    from backend.utils.ngwaf_bot_cache import ensure_schema as _ensure_cache_schema

    # Ensure the cache schema exists before any read attempts. Without this,
    # the very first sync (cache file is 0 bytes) sees the LEFT JOIN against
    # ngwaf_bots throw, returns None, exits the cron, and the schema is never
    # created — leaving the sync permanently broken.
    try:
        _ensure_cache_schema()
    except Exception:
        pass

    ngwaf_db = svcconfig.ngwaf_db_path()
    table_name = _safe_table(src["name"])

    if not os.path.exists(ngwaf_db):
        return None

    try:
        # read_only: ATTACH + SELECT only, no writes.
        con = get_connection(source=src, max_wait=5, skip_view_update=True, read_only=True)
        from backend.repositories._base import attach_ngwaf_cache

        # We don't have actual_cols handy, but we know waf_req_id is required
        # so we pass it explicitly to force the attachment.
        with attach_ngwaf_cache(con, ["waf_req_id"], alias="_ngwaf_oldest") as attached:
            if attached:
                row = con.execute(f"""
                    SELECT MIN(t.timestamp)
                    FROM {table_name} t
                    LEFT JOIN _ngwaf_oldest.ngwaf_bots nb USING (waf_req_id)
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
        con.close()
    except Exception:
        pass

    return None
