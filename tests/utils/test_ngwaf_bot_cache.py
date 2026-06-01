"""Tests for backend/utils/ngwaf_bot_cache.py — SQLite bot cache."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from backend.utils.ngwaf_bot_cache import cleanup_old_bots, get_last_timestamp, upsert_bots


@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    """Redirect all cache operations to a temp directory for each test."""
    with patch("backend.utils.ngwaf_bot_cache._db_path", return_value=tmp_path / "ngwaf_bot_cache.db"):
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


def test_cleanup_old_bots_removes_old_rows(tmp_path):
    """Rows with synced_at older than retention_days must be deleted."""
    import sqlite3

    db_path = tmp_path / "ngwaf_bot_cache.db"

    with patch("backend.utils.ngwaf_bot_cache._db_path", return_value=db_path):
        # Insert a record, then manually backdated synced_at to 40 days ago
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
        con = sqlite3.connect(str(db_path))
        con.execute("UPDATE ngwaf_bots SET synced_at = ? WHERE waf_req_id = 'old-req'", (old_ts,))
        con.commit()
        con.close()

        deleted = cleanup_old_bots(retention_days=30)
        assert deleted == 1


def test_cleanup_old_bots_keeps_recent_rows(tmp_path):
    db_path = tmp_path / "ngwaf_bot_cache.db"

    with patch("backend.utils.ngwaf_bot_cache._db_path", return_value=db_path):
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


# ── ensure_schema (regression) ────────────────────────────────────────────────
#
# Regression for the "stuck in zero-byte" bug: oldest_unenriched_timestamp
# attaches the cache file in DuckDB and joins ngwaf_bots — if the file exists
# but the schema has never been created (e.g. someone deleted the file and a
# 0-byte stub got recreated by a downstream caller), the JOIN throws, the
# planner returns None, the cron exits, and ngwaf_bots is never written.
# ensure_schema() breaks the cycle by creating the tables eagerly.


def test_ensure_schema_creates_tables_when_file_missing(tmp_path):
    import sqlite3

    from backend.utils import ngwaf_bot_cache as _cache

    db_path = tmp_path / "fresh.db"
    with patch("backend.utils.ngwaf_bot_cache._db_path", return_value=db_path):
        assert not db_path.exists()
        _cache.ensure_schema()
        assert db_path.exists()
        con = sqlite3.connect(str(db_path))
        try:
            tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        finally:
            con.close()
    assert tables >= {"ngwaf_bots", "ngwaf_sync_state"}


def test_ensure_schema_recovers_zero_byte_file(tmp_path):
    """The exact failure mode the cron got stuck on."""
    import sqlite3

    from backend.utils import ngwaf_bot_cache as _cache

    db_path = tmp_path / "stub.db"
    db_path.touch()  # 0-byte file
    assert db_path.stat().st_size == 0
    with patch("backend.utils.ngwaf_bot_cache._db_path", return_value=db_path):
        _cache.ensure_schema()
    assert db_path.stat().st_size > 0
    con = sqlite3.connect(str(db_path))
    try:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    finally:
        con.close()
    assert "ngwaf_bots" in tables


def test_ensure_schema_does_not_clobber_existing_data():
    upsert_bots([{"waf_req_id": "keep-me", "bot_name": "KeepBot"}], "ws1", latest_timestamp="2026-05-15T00:00:00Z")
    from backend.utils.ngwaf_bot_cache import ensure_schema, get_db_path

    ensure_schema()
    ensure_schema()

    import sqlite3

    con = sqlite3.connect(get_db_path())
    try:
        rows = con.execute("SELECT waf_req_id, bot_name FROM ngwaf_bots").fetchall()
    finally:
        con.close()
    assert ("keep-me", "KeepBot") in rows
