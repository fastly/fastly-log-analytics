"""Tests for backend.utils.date_utils.parse_date_window.

This utility converts start/end date strings (which might be ISO-8601,
YYYY-MM-DD, or garbage) into a canonical YYYY-MM-DDTHH:MM:SSZ window.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from backend.utils.date_utils import iso_z_now, parse_date_window, parse_window_str_to_dt


def test_iso_z_now_returns_iso8601_z_format():
    """Pinned because rdns_cache and other callers compare this output
    against the SQL filter ``looked_up_at < datetime('now', '-48 hours')``
    — using a different format would silently break the stale-entry
    refresh path."""
    out = iso_z_now()
    assert out.endswith("Z")
    assert "T" in out
    assert len(out) == 20  # YYYY-MM-DDTHH:MM:SSZ


def test_explicit_iso_strings_passthrough():
    start, end = parse_date_window("2026-05-01T00:00:00Z", "2026-05-02T00:00:00Z")
    assert start == "2026-05-01T00:00:00Z"
    assert end == "2026-05-02T00:00:00Z"


def test_date_only_end_extends_to_end_of_day():
    """Per the impl: when ``end`` is YYYY-MM-DD only, it's bumped to 23:59:59
    so the window includes the whole day."""
    _, end = parse_date_window("2026-05-01T00:00:00Z", "2026-05-02")
    assert end == "2026-05-02T23:59:59Z"


def test_invalid_start_falls_back_to_default_window():
    """Garbage start → default of (now - default_days)."""
    fixed_now = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
    with patch("backend.utils.date_utils.datetime") as m:
        m.now.return_value = fixed_now
        m.fromisoformat.side_effect = ValueError
        m.strptime.side_effect = ValueError
        start, _ = parse_date_window("garbage", "also-garbage", default_days=7)
    expected = (fixed_now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert start == expected


def test_empty_strings_use_defaults():
    """Empty strings short-circuit to the (now - default_days, now) window.
    Note: end is bumped to 23:59:59 because ``len("") <= 10`` triggers the
    end-of-day extension branch — same code path as a date-only YYYY-MM-DD
    end. That's intentional given the impl, so we assert it explicitly.
    """
    fixed_now = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
    with patch("backend.utils.date_utils.datetime") as m:
        m.now.return_value = fixed_now
        start, end = parse_date_window("", "", default_days=3)
    assert start == (fixed_now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # End-of-day bump applies because the input length triggers it
    assert end == "2026-05-15T23:59:59Z"


def test_naive_iso_string_assumes_utc():
    """A timestamp without tz info gets UTC stamped on it (per _parse_dt)."""
    start, _ = parse_date_window("2026-05-01T12:00:00", "2026-05-02")
    assert start == "2026-05-01T12:00:00Z"


def test_offset_timezone_converted_to_utc():
    """A +05:30 timestamp converts to UTC for the canonical form."""
    start, _ = parse_date_window("2026-05-01T12:00:00+05:30", "2026-05-02")
    # 12:00 IST → 06:30 UTC
    assert start == "2026-05-01T06:30:00Z"


# ── parse_window_str_to_dt: round-trip with parse_date_window output ───────


def test_parse_window_str_to_dt_round_trips_with_parse_date_window():
    """REGRESSION (commit 9af3c8f/deca5a0 follow-up): ``parse_date_window``
    returns ISO-T-Z strings; callers must NOT do
    ``strptime("%Y-%m-%d %H:%M:%S")`` on them — that crashes with
    ValueError. ``parse_window_str_to_dt`` is the canonical parser.

    Before this helper existed, ``backend/routers/usage.py`` had 6
    strptime sites with the wrong format string. Each would 500 the
    /api/usage/operations and /api/usage/log-activity endpoints
    whenever the user requested Fastly-stats correlation.
    """
    start_str, end_str = parse_date_window("2026-05-01T00:00:00Z", "2026-05-02T00:00:00Z")
    start_dt = parse_window_str_to_dt(start_str)
    end_dt = parse_window_str_to_dt(end_str)

    assert start_dt.year == 2026 and start_dt.month == 5 and start_dt.day == 1
    assert end_dt.year == 2026 and end_dt.month == 5 and end_dt.day == 2
    assert start_dt.tzinfo == UTC
    assert end_dt.tzinfo == UTC
    # The downstream usage of ``.timestamp()`` works without crashing
    assert isinstance(start_dt.timestamp(), float)
    assert isinstance(end_dt.timestamp(), float)


def test_parse_window_str_to_dt_handles_z_suffix():
    """The ``Z`` suffix → UTC. fromisoformat doesn't parse Z natively
    on older Python versions; the helper does the swap explicitly."""
    dt = parse_window_str_to_dt("2026-05-15T12:34:56Z")
    assert dt.tzinfo == UTC
    assert dt.hour == 12


def test_parse_window_str_to_dt_handles_offset_format():
    """+00:00 → UTC. Pinned because both ``Z`` and ``+00:00`` are
    canonical ISO; supporting both means the helper survives a
    refactor of parse_date_window that swaps suffixes."""
    dt = parse_window_str_to_dt("2026-05-15T12:34:56+00:00")
    assert dt.tzinfo == UTC


def test_parse_window_str_to_dt_raises_on_invalid_input():
    """Invalid input → ValueError. Distinct from the safe-default
    behaviour of _parse_dt — this helper is for trusted input from
    parse_date_window's output."""
    with pytest.raises(ValueError):
        parse_window_str_to_dt("not-an-iso-string")
