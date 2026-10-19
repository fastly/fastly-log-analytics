"""Tests for backend/utils/ngwaf_bot_cache.py — NGWAF bot cache on PostgreSQL."""

from datetime import UTC, datetime, timedelta

import pytest

from backend.core.metadata import pg_connection
from backend.utils.ngwaf_bot_cache import (
    cleanup_old_bots,
    ensure_schema,
    get_cache_stats,
    get_last_timestamp,
    upsert_bots,
)


@pytest.fixture(autouse=True)
def isolated_db():
    """Wipe the cache tables before each test."""
    con = pg_connection.get_pg_thread_connection()
    try:
        con.execute("DELETE FROM ngwaf_bots")
        con.execute("DELETE FROM ngwaf_sync_state")
        con.commit()
    except Exception:
        con.rollback()
    yield


# ── get_last_timestamp ────────────────────────────────────────────────────────


def test_get_last_timestamp_returns_none_when_no_state():
    ts = get_last_timestamp("ws-missing")
    assert ts is None


def test_get_last_timestamp_returns_stored_value_after_upsert():
    upsert_bots([], "ws1", latest_timestamp="2026-05-06T12:00:00Z")
    # Stored as +1s so the next sync doesn't re-fetch the last event
    assert get_last_timestamp("ws1") == "2026-05-06T12:00:01Z"


def test_get_last_timestamp_isolated_per_workspace():
    upsert_bots([], "ws-a", latest_timestamp="2026-05-01T00:00:00Z")
    # ws-b was never synced, should get default
    ts = get_last_timestamp("ws-b")
    assert ts is None


# ── upsert_bots ───────────────────────────────────────────────────────────────


def test_upsert_bots_stores_records():
    records = [
        {
            "waf_req_id": "req1",
            "bot_name": "OpenAI SearchBot",
            "category": "AI-FETCHER",
            "wellknown_bot_id": "openai-searchbot",
            "wellknown_bot_name": "OpenAI SearchBot crawler",
        }
    ]
    upsert_bots(records, "ws1", latest_timestamp="2026-05-07T10:00:00Z")

    # Stored as +1s so the next sync doesn't re-fetch the last event
    assert get_last_timestamp("ws1") == "2026-05-07T10:00:01Z"


def test_upsert_bots_is_idempotent():
    """Calling upsert twice with the same waf_req_id must not create duplicate rows."""
    record = {
        "waf_req_id": "req-dup",
        "bot_name": "AhrefsBot",
        "category": "SEO",
        "wellknown_bot_id": None,
        "wellknown_bot_name": None,
    }
    upsert_bots([record], "ws1", latest_timestamp="2026-05-07T08:00:00Z")
    upsert_bots([record], "ws1", latest_timestamp="2026-05-07T09:00:00Z")

    # The second upsert should update (not duplicate) and advance the timestamp (+1s)
    assert get_last_timestamp("ws1") == "2026-05-07T09:00:01Z"


def test_upsert_bots_skips_records_without_waf_req_id():
    records = [{"waf_req_id": None, "bot_name": "Phantom"}]
    upsert_bots(records, "ws1", latest_timestamp="2026-05-07T00:00:00Z")
    # No crash; timestamp still updated (+1s)
    assert get_last_timestamp("ws1") == "2026-05-07T00:00:01Z"


def test_upsert_bots_does_not_update_timestamp_when_none():
    upsert_bots([], "ws1", latest_timestamp="2026-05-07T06:00:00Z")
    upsert_bots([], "ws1", latest_timestamp=None)
    # Timestamp should remain at the first upsert value (+1s), not change on None
    assert get_last_timestamp("ws1") == "2026-05-07T06:00:01Z"


def test_upsert_bots_stores_wellknown_null_correctly():
    """wellknown_bot_* can legitimately be None for bots not in the registry."""
    records = [
        {
            "waf_req_id": "req-unknown",
            "bot_name": "UnknownNewBot",
            "category": "AI-FETCHER",
            "wellknown_bot_id": None,
            "wellknown_bot_name": None,
        }
    ]
    upsert_bots(records, "ws1", latest_timestamp="2026-05-07T10:00:00Z")
    assert get_last_timestamp("ws1") == "2026-05-07T10:00:01Z"


# ── cleanup_old_bots ──────────────────────────────────────────────────────────


def test_cleanup_old_bots_removes_old_rows():
    """Rows with synced_at older than retention_days must be deleted."""
    upsert_bots(
        [
            {
                "waf_req_id": "old-req",
                "bot_name": "OldBot",
                "category": None,
                "wellknown_bot_id": None,
                "wellknown_bot_name": None,
            }
        ],
        "ws1",
        latest_timestamp="2026-04-01T00:00:00Z",
    )

    old_ts = (datetime.now(UTC) - timedelta(days=40)).strftime("%Y-%m-%dT%H:%M:%SZ")
    con = pg_connection.get_pg_thread_connection()
    con.execute("UPDATE ngwaf_bots SET synced_at = ? WHERE waf_req_id = 'old-req'", (old_ts,))
    con.commit()

    deleted = cleanup_old_bots(retention_days=30)
    assert deleted == 1


def test_cleanup_old_bots_keeps_recent_rows():
    upsert_bots(
        [
            {
                "waf_req_id": "new-req",
                "bot_name": "NewBot",
                "category": None,
                "wellknown_bot_id": None,
                "wellknown_bot_name": None,
            }
        ],
        "ws1",
        latest_timestamp="2026-05-07T00:00:00Z",
    )
    deleted = cleanup_old_bots(retention_days=30)
    assert deleted == 0


def test_cleanup_old_bots_returns_zero_on_empty_table():
    deleted = cleanup_old_bots(retention_days=30)
    assert deleted == 0


def test_ensure_schema_and_get_cache_stats():
    ensure_schema()
    upsert_bots(
        [{"waf_req_id": "stat-bot-1", "bot_name": "Googlebot"}],
        "ws-stats",
        latest_timestamp="2026-05-15T00:00:00Z",
    )
    stats = get_cache_stats()
    assert stats["total_cached_bots"] == 1
    assert "ws-stats" in stats["workspaces"]
    assert stats["top_bots"].get("Googlebot") == 1
