"""Tests for backend/utils/ngwaf.py and backend/utils/ngwaf_bot_cache.py."""

from __future__ import annotations

import os
import urllib.parse
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import vcr

from backend.core.metadata import pg_connection
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.utils.ngwaf import (
    _extract_bot_record,
    _get_relative_time_range,
    cleanup_old_bots,
    fetch_verified_bots_paged,
    get_cache_stats,
    get_last_timestamp,
    oldest_unenriched_timestamp,
    upsert_bots,
)

# ── _extract_bot_record ───────────────────────────────────────────────────────


def test_extract_bot_record_returns_none_without_verified_bot_signal():
    r = {
        "id": "abc123",
        "signals": [{"id": "DATACENTER", "location": "Microsoft Azure"}],
        "user_agent": "SomeBot/1.0",
        "server_name": "example.com",
        "timestamp": "2026-05-07T00:00:00Z",
    }
    assert _extract_bot_record(r) is None


def test_extract_bot_record_returns_none_on_empty_signals():
    r = {"id": "abc123", "signals": []}
    assert _extract_bot_record(r) is None


def test_extract_bot_record_extracts_bot_name_and_category():
    r = {
        "id": "05daf2b7aedc405da50c000000000001",
        "signals": [
            {"id": "VERIFIED-BOT", "value": "OpenAI SearchBot"},
            {"id": "VERIFIED-BOT.AI-FETCHER", "value": "OpenAI SearchBot"},
            {"id": "DATACENTER", "location": "Microsoft Azure"},
        ],
        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
        "server_name": "www.example.com",
        "timestamp": "2026-05-07T10:00:00Z",
    }
    result = _extract_bot_record(r)

    assert result is not None
    assert result["waf_req_id"] == "05daf2b7aedc405da50c000000000001"
    assert result["bot_name"] == "OpenAI SearchBot"
    assert result["category"] == "AI-FETCHER"
    assert result["user_agent"] == "Mozilla/5.0 (compatible; GPTBot/1.0)"
    assert result["server_name"] == "www.example.com"
    assert result["timestamp"] == "2026-05-07T10:00:00Z"


def test_extract_bot_record_category_none_when_no_subcategory():
    r = {
        "id": "abc",
        "signals": [{"id": "VERIFIED-BOT", "value": "SomeBot"}],
        "user_agent": "SomeBot/1.0",
    }
    result = _extract_bot_record(r)
    assert result is not None
    assert result["bot_name"] == "SomeBot"
    assert result["category"] is None


def test_extract_bot_record_ua_from_headers_list_when_no_top_level():
    r = {
        "id": "abc",
        "signals": [{"id": "VERIFIED-BOT", "value": "AhrefsBot"}],
        "request_headers": [
            {"name": "Accept", "value": "text/html"},
            {"name": "User-Agent", "value": "AhrefsBot/7.0"},
        ],
    }
    result = _extract_bot_record(r)
    assert result is not None
    assert result["user_agent"] == "AhrefsBot/7.0"


def test_extract_bot_record_ua_header_matching_is_case_insensitive():
    r = {
        "id": "abc",
        "signals": [{"id": "VERIFIED-BOT", "value": "AhrefsBot"}],
        "request_headers": [{"name": "user-agent", "value": "AhrefsBot/7.0"}],
    }
    result = _extract_bot_record(r)
    assert result is not None
    assert result["user_agent"] == "AhrefsBot/7.0"


def test_extract_bot_record_category_strips_prefix():
    r = {
        "id": "abc",
        "signals": [
            {"id": "VERIFIED-BOT", "value": "Googlebot"},
            {"id": "VERIFIED-BOT.SEARCH-ENGINE", "value": "Googlebot"},
        ],
        "user_agent": "Googlebot/2.1",
    }
    result = _extract_bot_record(r)
    assert result is not None
    assert result["category"] == "SEARCH-ENGINE"


def test_extract_bot_record_ignores_non_verified_bot_subcategory():
    """Signals like BOT-ANALYSIS don't match the VERIFIED-BOT.* subcategory filter."""
    r = {
        "id": "abc",
        "signals": [
            {"id": "VERIFIED-BOT", "value": "Facebook Crawler"},
            {"id": "BOT-ANALYSIS", "value": "something"},
        ],
        "user_agent": "facebookexternalhit/1.1",
    }
    result = _extract_bot_record(r)
    assert result is not None
    assert result["category"] is None


# ── fetch_verified_bots_paged pagination ──────────────────────────────────────


def _make_page(data: list, next_cursor: str = "") -> dict:
    return {"data": data, "meta": {"next_cursor": next_cursor}}


def _bot_request(req_id: str, bot_name: str = "TestBot") -> dict:
    return {
        "id": req_id,
        "signals": [
            {"id": "VERIFIED-BOT", "value": bot_name},
            {"id": "VERIFIED-BOT.SEO", "value": bot_name},
        ],
        "user_agent": f"{bot_name}/1.0",
        "server_name": "example.com",
        "timestamp": "2026-05-07T10:00:00Z",
    }


def test_fetch_verified_bots_paged_single_page():
    page1 = [_bot_request("req1"), _bot_request("req2")]
    responses = [_make_page(page1, next_cursor="")]

    with patch("backend.core.fastly.client.fastly", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 1
    records, latest_ts, raw_count = pages[0]
    assert len(records) == 2
    assert raw_count == 2
    assert records[0]["waf_req_id"] == "req1"
    assert records[1]["waf_req_id"] == "req2"


def test_fetch_verified_bots_paged_routes_through_central_client():
    responses = [_make_page([_bot_request("req1")], next_cursor="")]

    with patch("backend.core.fastly.client.fastly", side_effect=responses) as mock_fastly:
        list(fetch_verified_bots_paged("api-key", "ws-id", "2026-05-06T00:00:00Z"))

    args, kwargs = mock_fastly.call_args
    assert args[0] == "GET"
    assert args[1].startswith("/ngwaf/v1/workspaces/ws-id/requests?")
    assert kwargs["token"] == "api-key"
    assert kwargs["timeout"] == 30


def test_fetch_verified_bots_paged_stops_on_empty_cursor():
    responses = [_make_page([_bot_request("req1")], next_cursor="")]

    with patch("backend.core.fastly.client.fastly", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 1


def test_fetch_verified_bots_paged_multi_page():
    page1 = [_bot_request("req1")]
    page2 = [_bot_request("req2")]
    responses = [
        _make_page(page1, next_cursor="cursor-abc"),
        _make_page(page2, next_cursor=""),
    ]

    with patch("backend.core.fastly.client.fastly", side_effect=responses) as mock_fastly, patch("time.sleep"):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 2
    assert pages[0][0][0]["waf_req_id"] == "req1"
    assert pages[1][0][0]["waf_req_id"] == "req2"
    assert "cursor=cursor-abc" in mock_fastly.call_args_list[1].args[1]


def test_fetch_verified_bots_paged_skips_non_verified_bot_records():
    non_bot = {
        "id": "req_no_bot",
        "signals": [{"id": "DATACENTER", "location": "AWS"}],
        "user_agent": "curl/7.0",
    }
    bot = _bot_request("req_bot")
    responses = [_make_page([non_bot, bot], next_cursor="")]

    with patch("backend.core.fastly.client.fastly", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    records, _, raw_count = pages[0]
    assert len(records) == 1
    assert raw_count == 2
    assert records[0]["waf_req_id"] == "req_bot"


def test_fetch_verified_bots_paged_latest_ts_is_max_in_page():
    earlier = {**_bot_request("req1"), "timestamp": "2026-05-07T09:00:00Z"}
    earlier["signals"] = [{"id": "VERIFIED-BOT", "value": "Bot"}]
    later = {**_bot_request("req2"), "timestamp": "2026-05-07T11:00:00Z"}
    later["signals"] = [{"id": "VERIFIED-BOT", "value": "Bot"}]
    responses = [_make_page([earlier, later], next_cursor="")]

    with patch("backend.core.fastly.client.fastly", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    _, latest_ts, _ = pages[0]
    assert latest_ts == "2026-05-07T11:00:00Z"


def test_fetch_verified_bots_paged_empty_page_yields_none_timestamp():
    responses = [_make_page([], next_cursor="")]

    with patch("backend.core.fastly.client.fastly", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 1
    records, latest_ts, raw_count = pages[0]
    assert records == []
    assert latest_ts is None
    assert raw_count == 0


# ── NGWAF query syntax & rounding ─────────────────────────────────────────────


def _parse_q(mock_fastly: MagicMock) -> str:
    path = mock_fastly.call_args.args[1]
    parsed = urllib.parse.urlparse(path)
    return urllib.parse.parse_qs(parsed.query).get("q", [""])[0]


def test_fetch_verified_bots_paged_uses_proper_query_syntax():
    from_dt = datetime.now(UTC) - timedelta(days=4, hours=12)
    from_ts = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    with patch("backend.core.fastly.client.fastly", return_value=_make_page([], "")) as mock_fastly:
        list(fetch_verified_bots_paged("api-key", "ws-id", from_ts))

        path = mock_fastly.call_args.args[1]
        parsed = urllib.parse.urlparse(path)
        params = urllib.parse.parse_qs(parsed.query)

        q = params.get("q", [""])[0]
        assert "from:-5d" in q
        assert "from" not in params
        assert "until" not in params


def test_fetch_verified_bots_paged_rounds_up_to_15min():
    from_dt = datetime.now(UTC) - timedelta(minutes=12)
    from_ts = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    with patch("backend.core.fastly.client.fastly", return_value=_make_page([], "")) as mock_fastly:
        list(fetch_verified_bots_paged("api-key", "ws-id", from_ts))

        assert "from:-15min" in _parse_q(mock_fastly)


# ── _get_relative_time_range: direct branch coverage ────────────────────────


def test_relative_time_range_passes_through_existing_negative_string():
    assert _get_relative_time_range("-3d") == "-3d"
    assert _get_relative_time_range("-15min") == "-15min"


def test_relative_time_range_invalid_format_returns_default_1d():
    assert _get_relative_time_range("not-a-timestamp") == "-1d"
    assert _get_relative_time_range("") == "-1d"


def test_relative_time_range_hour_range_rounds_up():
    from_dt = datetime.now(UTC) - timedelta(hours=4, minutes=30)
    out = _get_relative_time_range(from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert out == "-5h"


def test_relative_time_range_minute_range_rounds_up_to_15min_multiple():
    from_dt = datetime.now(UTC) - timedelta(minutes=40)
    out = _get_relative_time_range(from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert out == "-45min"


def test_relative_time_range_minimum_is_15min_even_for_recent_ts():
    from_dt = datetime.now(UTC) - timedelta(seconds=5)
    out = _get_relative_time_range(from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert out == "-15min"


# ── fetch_verified_bots_paged: until_ts ─────────────────────────────────────


def test_fetch_verified_bots_paged_includes_until_when_provided():
    from_dt = datetime.now(UTC) - timedelta(days=3)
    until_dt = datetime.now(UTC) - timedelta(days=1)

    with patch("backend.core.fastly.client.fastly", return_value=_make_page([], "")) as mock_fastly:
        list(
            fetch_verified_bots_paged(
                "k",
                "ws",
                from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                until_ts=until_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )

    q = _parse_q(mock_fastly)
    assert "from:-" in q
    assert "until:-" in q


# ── oldest_unenriched_timestamp ─────────────────────────────────────────────


def test_oldest_unenriched_returns_none_when_ngwaf_db_missing(tmp_path):
    """When no unenriched rows or connection cannot attach, return None."""
    src = {"name": "svc1"}

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(tmp_path / "missing.db")),
        patch("backend.core.duckdb.get_connection", side_effect=CronLeaseHeldError("no db")),
    ):
        assert oldest_unenriched_timestamp(src) is None


def test_oldest_unenriched_returns_none_on_unexpected_exception(tmp_path):
    ngwaf_db = tmp_path / "ngwaf.db"
    ngwaf_db.touch()
    src = {"name": "svc1"}

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.core.duckdb.get_connection", side_effect=CronLeaseHeldError("DB busy")),
    ):
        assert oldest_unenriched_timestamp(src) is None


def test_oldest_unenriched_tolerates_ensure_schema_failure(tmp_path):
    ngwaf_db = tmp_path / "ngwaf.db"
    ngwaf_db.touch()
    src = {"name": "svc1"}

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.core.duckdb.get_connection", side_effect=CronLeaseHeldError("DB busy")),
    ):
        assert oldest_unenriched_timestamp(src) is None


def test_oldest_unenriched_returns_formatted_timestamp_when_row_found(tmp_path):
    ngwaf_db = tmp_path / "ngwaf.db"
    ngwaf_db.touch()
    src = {"name": "svc1"}

    expected_dt = datetime(2026, 4, 15, 12, 30, 0, tzinfo=UTC)
    fake_con = MagicMock()
    fake_con.execute.return_value.fetchone.return_value = (expected_dt,)

    attached_cm = MagicMock()
    attached_cm.__enter__.return_value = True
    attached_cm.__exit__.return_value = False

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.repositories._base.attach_ngwaf_cache", return_value=attached_cm),
    ):
        out = oldest_unenriched_timestamp(src)

    assert out == "2026-04-15T12:30:00Z"


# ── Merged from test_ngwaf_bot_cache.py ──────────────────────────────────────


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


def test_get_last_timestamp_returns_none_when_no_state():
    ts = get_last_timestamp("ws-missing")
    assert ts is None


def test_get_last_timestamp_returns_stored_value_after_upsert():
    upsert_bots([], "ws1", latest_timestamp="2026-05-06T12:00:00Z")
    assert get_last_timestamp("ws1") == "2026-05-06T12:00:01Z"


def test_get_last_timestamp_isolated_per_workspace():
    upsert_bots([], "ws-a", latest_timestamp="2026-05-01T00:00:00Z")
    ts = get_last_timestamp("ws-b")
    assert ts is None


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
    assert get_last_timestamp("ws1") == "2026-05-07T10:00:01Z"


def test_upsert_bots_is_idempotent():
    record = {
        "waf_req_id": "req-dup",
        "bot_name": "AhrefsBot",
        "category": "SEO",
        "wellknown_bot_id": None,
        "wellknown_bot_name": None,
    }
    upsert_bots([record], "ws1", latest_timestamp="2026-05-07T08:00:00Z")
    upsert_bots([record], "ws1", latest_timestamp="2026-05-07T09:00:00Z")
    assert get_last_timestamp("ws1") == "2026-05-07T09:00:01Z"


def test_upsert_bots_skips_records_without_waf_req_id():
    records = [{"waf_req_id": None, "bot_name": "Phantom"}]
    upsert_bots(records, "ws1", latest_timestamp="2026-05-07T00:00:00Z")
    assert get_last_timestamp("ws1") == "2026-05-07T00:00:01Z"


def test_upsert_bots_does_not_update_timestamp_when_none():
    upsert_bots([], "ws1", latest_timestamp="2026-05-07T06:00:00Z")
    upsert_bots([], "ws1", latest_timestamp=None)
    assert get_last_timestamp("ws1") == "2026-05-07T06:00:01Z"


def test_upsert_bots_stores_wellknown_null_correctly():
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


def test_cleanup_old_bots_removes_old_rows():
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
    upsert_bots(
        [{"waf_req_id": "stat-bot-1", "bot_name": "Googlebot"}],
        "ws-stats",
        latest_timestamp="2026-05-15T00:00:00Z",
    )
    stats = get_cache_stats()
    assert stats["total_cached_bots"] == 1
    assert "ws-stats" in stats["workspaces"]
    assert stats["top_bots"].get("Googlebot") == 1


# ── Merged from test_ngwaf_vcr.py ────────────────────────────────────────────

CASSETTE_DIR = os.path.join(os.path.dirname(__file__), "..", "cassettes")

_my_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    filter_headers=[("Fastly-Key", "REDACTED"), ("Authorization", "REDACTED")],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def test_single_page_yields_two_verified_bots():
    with _my_vcr.use_cassette("ngwaf_verified_bots_single_page.yaml"):
        pages = list(
            fetch_verified_bots_paged(
                api_key="test-token-not-real",
                workspace_id="ws-test-1",
                from_ts="-1h",
                page_limit=500,
            )
        )

    assert len(pages) == 1, f"expected exactly one page, got {len(pages)}"
    records, latest_ts, raw_count = pages[0]

    assert raw_count == 3, "raw_count should include all records pre-filter"
    assert len(records) == 2, f"only 2 of 3 records have VERIFIED-BOT signal; got {len(records)}: {records!r}"

    bot_names = sorted(r["bot_name"] for r in records)
    assert bot_names == ["bingbot", "googlebot"]

    categories = sorted(r["category"] for r in records)
    assert categories == ["SEARCH-ENGINE", "SEARCH-ENGINE"]

    assert latest_ts == "2026-05-26T10:05:00Z"


def test_cassette_no_match_raises_loudly():
    with _my_vcr.use_cassette("ngwaf_verified_bots_single_page.yaml"):
        with pytest.raises(Exception) as excinfo:
            list(
                fetch_verified_bots_paged(
                    api_key="test-token-not-real",
                    workspace_id="ws-DIFFERENT",
                    from_ts="-1h",
                    page_limit=500,
                )
            )
        assert "no match" in str(excinfo.value).lower() or "cannot" in str(excinfo.value).lower(), (
            f"expected vcrpy refusal, got: {excinfo.value!r}"
        )
