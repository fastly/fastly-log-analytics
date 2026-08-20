"""Tests for date and time parsing utilities."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.utils.date_utils import (
    _parse_dt,
    iso_z,
    iso_z_now,
    parse_date_window,
    parse_iso_utc,
    parse_relative_time_window,
    parse_window_str_to_dt,
    safe_iso,
    window_to_epoch,
)


def test_parse_iso_utc():
    # 1. Empty/None inputs
    assert parse_iso_utc(None) is None
    assert parse_iso_utc("") is None

    # 2. ISO format with timezone or Z
    dt1 = parse_iso_utc("2026-08-19T12:00:00Z")
    assert dt1 is not None
    assert dt1.tzinfo == UTC
    assert dt1.hour == 12

    # 3. Naive date format
    dt2 = parse_iso_utc("2026-08-19")
    assert dt2 is not None
    assert dt2.tzinfo == UTC
    assert dt2.hour == 0
    assert dt2.day == 19

    # 4. Invalid inputs
    assert parse_iso_utc("invalid-date-string") is None


def test_iso_z():
    dt = datetime(2026, 8, 19, 12, 34, 56, tzinfo=UTC)
    assert iso_z(dt) == "2026-08-19T12:34:56Z"


def test_iso_z_now():
    now_str = iso_z_now()
    assert now_str.endswith("Z")
    assert "T" in now_str


def test_parse_dt_helper():
    default_val = datetime(2020, 1, 1, tzinfo=UTC)
    # Success path
    assert _parse_dt("2026-08-19T12:00:00Z", default_val).year == 2026
    # Fallback path
    assert _parse_dt("invalid", default_val) == default_val


def test_parse_date_window():
    # Standard 10-char date triggers hour/minute/second replace on end_dt
    start, end = parse_date_window("2026-08-10", "2026-08-19")
    assert start == "2026-08-10T00:00:00Z"
    assert end == "2026-08-19T23:59:59Z"

    # Longer string doesn't replace hours/minutes/seconds
    start2, end2 = parse_date_window("2026-08-10T12:00:00Z", "2026-08-19T12:00:00Z")
    assert start2 == "2026-08-10T12:00:00Z"
    assert end2 == "2026-08-19T12:00:00Z"


def test_safe_iso():
    assert safe_iso(None) is None

    # Datetime-like object
    dt = datetime(2026, 8, 19, 12, 0, 0)
    assert safe_iso(dt) == "2026-08-19T12:00:00Z"

    # Naive string fallback
    assert safe_iso("2026-08-19") == "2026-08-19"


def test_parse_window_str_to_dt():
    dt = parse_window_str_to_dt("2026-08-19T12:00:00Z")
    assert dt.year == 2026
    assert dt.month == 8

    with pytest.raises(ValueError, match="Invalid date window string"):
        parse_window_str_to_dt("invalid")


def test_window_to_epoch():
    s, e, from_ts, to_ts = window_to_epoch("2026-08-10T00:00:00Z", "2026-08-19T12:00:00Z")
    assert s == "2026-08-10T00:00:00Z"
    assert e == "2026-08-19T12:00:00Z"
    assert from_ts > 0
    assert to_ts > from_ts


def test_parse_relative_time_window():
    # 1. Empty since defaults to 1h
    dt_empty = parse_relative_time_window("")
    assert (datetime.now(UTC) - dt_empty).total_seconds() == pytest.approx(3600, abs=10)

    # 2. Hours unit "24h"
    dt_hours = parse_relative_time_window("12h")
    assert (datetime.now(UTC) - dt_hours).total_seconds() == pytest.approx(12 * 3600, abs=10)

    # 3. Days unit "7d"
    dt_days = parse_relative_time_window("5d")
    assert (datetime.now(UTC) - dt_days).total_seconds() == pytest.approx(5 * 24 * 3600, abs=10)

    # 4. Minutes unit "30m"
    dt_mins = parse_relative_time_window("30m")
    assert (datetime.now(UTC) - dt_mins).total_seconds() == pytest.approx(30 * 60, abs=10)

    # 5. Invalid / parse fail defaults to 1h
    dt_invalid = parse_relative_time_window("invalid")
    assert (datetime.now(UTC) - dt_invalid).total_seconds() == pytest.approx(3600, abs=10)
