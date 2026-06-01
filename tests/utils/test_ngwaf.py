"""Tests for backend/utils/ngwaf.py — NGWAF API client."""

import json
import urllib.parse
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from backend.utils.ngwaf import (
    _extract_bot_record,
    _get_relative_time_range,
    fetch_verified_bots_paged,
    oldest_unenriched_timestamp,
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


def _make_mock_response(data: list, next_cursor: str = "") -> MagicMock:
    payload = {"data": data, "meta": {"next_cursor": next_cursor}}
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(payload).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


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
    responses = [_make_mock_response(page1, next_cursor="")]

    with patch("urllib.request.urlopen", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 1
    records, latest_ts, raw_count = pages[0]
    assert len(records) == 2
    assert raw_count == 2
    assert records[0]["waf_req_id"] == "req1"
    assert records[1]["waf_req_id"] == "req2"


def test_fetch_verified_bots_paged_stops_on_empty_cursor():
    """Empty string cursor must stop pagination (API uses '' not null)."""
    responses = [_make_mock_response([_bot_request("req1")], next_cursor="")]

    with patch("urllib.request.urlopen", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 1


def test_fetch_verified_bots_paged_multi_page():
    page1 = [_bot_request("req1")]
    page2 = [_bot_request("req2")]
    responses = [
        _make_mock_response(page1, next_cursor="cursor-abc"),
        _make_mock_response(page2, next_cursor=""),
    ]

    with patch("urllib.request.urlopen", side_effect=responses), patch("time.sleep"):  # skip the 100ms inter-page sleep
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 2
    assert pages[0][0][0]["waf_req_id"] == "req1"
    assert pages[1][0][0]["waf_req_id"] == "req2"


def test_fetch_verified_bots_paged_skips_non_verified_bot_records():
    """Records without VERIFIED-BOT signal are excluded from page_records."""
    non_bot = {
        "id": "req_no_bot",
        "signals": [{"id": "DATACENTER", "location": "AWS"}],
        "user_agent": "curl/7.0",
    }
    bot = _bot_request("req_bot")
    responses = [_make_mock_response([non_bot, bot], next_cursor="")]

    with patch("urllib.request.urlopen", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    records, _, raw_count = pages[0]
    assert len(records) == 1
    assert raw_count == 2  # 2 total, 1 filtered to VERIFIED-BOT
    assert records[0]["waf_req_id"] == "req_bot"


def test_fetch_verified_bots_paged_latest_ts_is_max_in_page():
    earlier = {**_bot_request("req1"), "timestamp": "2026-05-07T09:00:00Z"}
    earlier["signals"] = [{"id": "VERIFIED-BOT", "value": "Bot"}]
    later = {**_bot_request("req2"), "timestamp": "2026-05-07T11:00:00Z"}
    later["signals"] = [{"id": "VERIFIED-BOT", "value": "Bot"}]
    responses = [_make_mock_response([earlier, later], next_cursor="")]

    with patch("urllib.request.urlopen", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    _, latest_ts, _ = pages[0]
    assert latest_ts == "2026-05-07T11:00:00Z"


def test_fetch_verified_bots_paged_empty_page_yields_none_timestamp():
    responses = [_make_mock_response([], next_cursor="")]

    with patch("urllib.request.urlopen", side_effect=responses):
        pages = list(fetch_verified_bots_paged("key", "ws1", "2026-05-06T00:00:00Z"))

    assert len(pages) == 1
    records, latest_ts, raw_count = pages[0]
    assert records == []
    assert latest_ts is None
    assert raw_count == 0


# ── NGWAF query syntax & rounding ─────────────────────────────────────────────


def test_fetch_verified_bots_paged_uses_proper_query_syntax():
    # Setup: 4.5 days ago
    from_dt = datetime.now(UTC) - timedelta(days=4, hours=12)
    from_ts = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps({"data": [], "meta": {"next_cursor": ""}}).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        list(fetch_verified_bots_paged("api-key", "ws-id", from_ts))

        args, _ = mock_urlopen.call_args
        req = args[0]
        url = req.full_url

        # Parse the URL to check params
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)

        # Implementation should have:
        # q containing 'from:-5d'
        # no separate 'from' or 'until' params
        q = params.get("q", [""])[0]
        assert "from:-5d" in q
        assert "from" not in params
        assert "until" not in params


def test_fetch_verified_bots_paged_rounds_up_to_15min():
    # Setup: 12 minutes ago
    from_dt = datetime.now(UTC) - timedelta(minutes=12)
    from_ts = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps({"data": [], "meta": {"next_cursor": ""}}).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        list(fetch_verified_bots_paged("api-key", "ws-id", from_ts))

        args, _ = mock_urlopen.call_args
        req = args[0]
        parsed = urllib.parse.urlparse(req.full_url)
        params = urllib.parse.parse_qs(parsed.query)

        q = params.get("q", [""])[0]
        assert "from:-15min" in q


# ── _get_relative_time_range: direct branch coverage ────────────────────────


def test_relative_time_range_passes_through_existing_negative_string():
    """Already-relative inputs (``-3d``, ``-5min``) round-trip unchanged.
    Callers pass these when they want to skip the auto-rounding."""
    assert _get_relative_time_range("-3d") == "-3d"
    assert _get_relative_time_range("-15min") == "-15min"


def test_relative_time_range_invalid_format_returns_default_1d():
    """Garbage input → safe default. ``-1d`` won't lose data — it'll
    just over-fetch on the next page — whereas raising would kill the
    whole bot-sync cron."""
    assert _get_relative_time_range("not-a-timestamp") == "-1d"
    assert _get_relative_time_range("") == "-1d"


def test_relative_time_range_hour_range_rounds_up():
    """4.5 hours ago → ``-5h`` (math.ceil)."""
    from_dt = datetime.now(UTC) - timedelta(hours=4, minutes=30)
    out = _get_relative_time_range(from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert out == "-5h"


def test_relative_time_range_minute_range_rounds_up_to_15min_multiple():
    """40 minutes ago → ``-45min`` (next 15-min increment)."""
    from_dt = datetime.now(UTC) - timedelta(minutes=40)
    out = _get_relative_time_range(from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert out == "-45min"


def test_relative_time_range_minimum_is_15min_even_for_recent_ts():
    """``ts`` that's only ~5 seconds in the past → still ``-15min``,
    not ``-0min``. ``max(mins, 1)`` exists for clock-skew safety; the
    actual rounding-up to 15 happens before the max."""
    from_dt = datetime.now(UTC) - timedelta(seconds=5)
    out = _get_relative_time_range(from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    # Round up to nearest 15: 5s → 15min
    assert out == "-15min"


# ── fetch_verified_bots_paged: until_ts ─────────────────────────────────────


def test_fetch_verified_bots_paged_includes_until_when_provided():
    """``until_ts`` adds an ``until:`` clause to the q parameter. Used
    by backfill sync to limit the range when chunking."""
    from_dt = datetime.now(UTC) - timedelta(days=3)
    until_dt = datetime.now(UTC) - timedelta(days=1)

    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps({"data": [], "meta": {"next_cursor": ""}}).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        list(
            fetch_verified_bots_paged(
                "k",
                "ws",
                from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                until_ts=until_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )

    parsed = urllib.parse.urlparse(mock_urlopen.call_args[0][0].full_url)
    q = urllib.parse.parse_qs(parsed.query).get("q", [""])[0]
    assert "from:-" in q
    assert "until:-" in q


# ── oldest_unenriched_timestamp ─────────────────────────────────────────────


def test_oldest_unenriched_returns_none_when_ngwaf_db_missing(tmp_path):
    """No ngwaf_bot_cache.db file → no work to do, return None.
    The sync cron uses this to decide whether to back-fill."""
    src = {"name": "svc1"}

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(tmp_path / "missing.db")),
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
    ):
        assert oldest_unenriched_timestamp(src) is None


def test_oldest_unenriched_returns_none_on_unexpected_exception(tmp_path):
    """Any error (DuckDB connection failure, missing log table) → None.
    Pinned because the sync cron treats None as "skip this iteration"
    — a raise would abort the whole cron job."""
    ngwaf_db = tmp_path / "ngwaf.db"
    ngwaf_db.touch()  # Just needs to exist
    src = {"name": "svc1"}

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("DB busy")),
    ):
        assert oldest_unenriched_timestamp(src) is None


def test_oldest_unenriched_tolerates_ensure_schema_failure(tmp_path):
    """``ensure_schema`` errors are swallowed — the function may be
    called on a brand-new install where the cache file is 0 bytes;
    the first-sync path must still complete."""
    ngwaf_db = tmp_path / "ngwaf.db"
    ngwaf_db.touch()
    src = {"name": "svc1"}

    with (
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch(
            "backend.utils.ngwaf_bot_cache.ensure_schema",
            side_effect=RuntimeError("can't open cache"),
        ),
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("DB busy")),
    ):
        # Must not raise — ensure_schema error is caught
        assert oldest_unenriched_timestamp(src) is None


def test_oldest_unenriched_returns_formatted_timestamp_when_row_found(tmp_path):
    """Happy path: the LEFT JOIN finds a row with a min timestamp, the
    helper formats it as an ISO-Z string the NGWAF API accepts."""
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
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.repositories._base.attach_ngwaf_cache", return_value=attached_cm),
    ):
        out = oldest_unenriched_timestamp(src)

    assert out == "2026-04-15T12:30:00Z"
