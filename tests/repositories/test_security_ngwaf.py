"""Tests for the NGWAF bot cache integration in get_security_aggregates.

These tests cover:
- ngwaf_verified_bots and ngwaf_verified_bots_ts are populated from the SQLite JOIN
- Both fields are empty lists when the cache file does not exist
- Both fields are empty lists when waf_req_id is absent from the log schema
"""

import sqlite3
from datetime import UTC, datetime
from unittest.mock import patch

import duckdb
import pytest

from backend.repositories._base import _safe_table
from backend.repositories.security import get_security_aggregates

# ── Helpers ───────────────────────────────────────────────────────────────────

_WAF_REQ_ID = "05daf2b7aedc405da50c000000000001"
_BOT_NAME = "OpenAI SearchBot"
_CATEGORY = "AI-FETCHER"
_WK_NAME = "OpenAI SearchBot crawler"


def _create_log_table_with_waf(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Create a minimal log table that includes waf_req_id."""
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp   TIMESTAMPTZ,
            ip          VARCHAR,
            ua          VARCHAR,
            waf_req_id  VARCHAR,
            waf_sig     VARCHAR
        )
    """)
    now = datetime.now(UTC)
    con.execute(
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
        [now, "1.2.3.4", "GPTBot/1.0", _WAF_REQ_ID, "VERIFIED-BOT,VERIFIED-BOT.AI-FETCHER"],
    )
    # A second row without a matching waf_req_id (no bot cache entry)
    con.execute(
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
        [now, "5.6.7.8", "curl/7.0", "deadbeef" * 4, None],
    )


def _create_cache_db(db_path: str) -> None:
    """Populate a minimal ngwaf_bot_cache SQLite file."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ngwaf_bots (
            waf_req_id         TEXT PRIMARY KEY,
            bot_name           TEXT,
            category           TEXT,
            wellknown_bot_id   TEXT,
            wellknown_bot_name TEXT,
            synced_at          TEXT
        )
    """)
    con.execute(
        "INSERT INTO ngwaf_bots VALUES (?, ?, ?, ?, ?, ?)",
        (
            _WAF_REQ_ID,
            _BOT_NAME,
            _CATEGORY,
            "openai-searchbot",
            _WK_NAME,
            datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ),
    )
    con.commit()
    con.close()


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def con_with_waf_logs():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite; LOAD sqlite;")
    yield con
    con.close()


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_ngwaf_verified_bots_populated_from_cache(con_with_waf_logs, tmp_path):
    """ngwaf_verified_bots is non-empty when the cache has a matching waf_req_id."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    table_name = _safe_table("ngwaf_test_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_test_svc", "service_id": "ngwaf-test"}

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    bots = result.get("ngwaf_verified_bots", [])
    assert len(bots) == 1
    assert bots[0]["bot_name"] == _BOT_NAME
    assert bots[0]["wellknown_bot_name"] == _WK_NAME
    assert bots[0]["category"] == _CATEGORY
    assert bots[0]["request_count"] == 1


def test_ngwaf_verified_bots_ts_populated_from_cache(con_with_waf_logs, tmp_path):
    """ngwaf_verified_bots_ts contains time-bucketed counts for each bot_name."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    table_name = _safe_table("ngwaf_ts_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_ts_svc", "service_id": "ngwaf-ts"}

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    ts = result.get("ngwaf_verified_bots_ts", [])
    assert len(ts) >= 1
    assert ts[0]["bot_name"] == _BOT_NAME
    assert ts[0]["count"] == 1


def test_ngwaf_fields_empty_when_cache_file_missing(con_with_waf_logs, tmp_path):
    """Both NGWAF fields must be empty lists when ngwaf_bot_cache.db doesn't exist."""
    missing_path = str(tmp_path / "does_not_exist.db")

    table_name = _safe_table("ngwaf_nocache_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_nocache_svc", "service_id": "ngwaf-nocache"}

    with patch("backend.config.ngwaf_db_path", return_value=missing_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    assert result["ngwaf_verified_bots"] == []
    assert result["ngwaf_verified_bots_ts"] == []


def test_ngwaf_fields_empty_when_waf_req_id_column_absent(tmp_path):
    """Both NGWAF fields must be empty lists when waf_req_id is not in the log schema."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL sqlite; LOAD sqlite;")
        table_name = _safe_table("ngwaf_nowaf_svc")
        # Table without waf_req_id column
        con.execute(f"""
            CREATE TABLE {table_name} (
                timestamp TIMESTAMPTZ,
                ip        VARCHAR,
                ua        VARCHAR
            )
        """)
        now = datetime.now(UTC)
        con.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?)", [now, "1.2.3.4", "curl/7.0"])

        src = {"name": "ngwaf_nowaf_svc", "service_id": "ngwaf-nowaf"}

        with patch("backend.config.ngwaf_db_path", return_value=db_path):
            result = get_security_aggregates(
                con=con,
                src=src,
                start_time=None,
                end_time=None,
                filters={},
            )
    finally:
        con.close()

    assert result["ngwaf_verified_bots"] == []
    assert result["ngwaf_verified_bots_ts"] == []


def test_ngwaf_unmatched_waf_req_ids_not_in_result(con_with_waf_logs, tmp_path):
    """Rows whose waf_req_id is not in the cache produce no bot entries (INNER JOIN)."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    # Create an empty cache (no rows)
    _con = sqlite3.connect(db_path)
    _con.execute(
        "CREATE TABLE ngwaf_bots (waf_req_id TEXT PRIMARY KEY, bot_name TEXT, "
        "category TEXT, wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    _con.commit()
    _con.close()

    table_name = _safe_table("ngwaf_nomatch_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_nomatch_svc", "service_id": "ngwaf-nomatch"}

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    assert result["ngwaf_verified_bots"] == []
    assert result["ngwaf_verified_bots_ts"] == []
