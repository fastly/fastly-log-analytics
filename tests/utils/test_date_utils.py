"""Tests for date and time parsing utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from backend.core.rollups._common import parse_hour_token
from backend.utils.date_utils import (
    _parse_dt,
    ensure_utc,
    floor_to_day,
    floor_to_hour,
    iso_z,
    iso_z_now,
    parse_date_window,
    parse_day_key,
    parse_day_key_strict,
    parse_hour_key,
    parse_hour_key_strict,
    parse_iso_utc,
    parse_relative_time_window,
    parse_window,
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
    assert safe_iso("") is None

    # Datetime-like object
    dt = datetime(2026, 8, 19, 12, 0, 0)
    assert safe_iso(dt) == "2026-08-19T12:00:00Z"

    # Naive date string fallback
    assert safe_iso("2026-08-19") == "2026-08-19"

    # Space-separated timestamp string (e.g. from ClickHouse JSON)
    assert safe_iso("2026-08-19 12:00:00") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19 12:00:00.123") == "2026-08-19T12:00:00.123Z"

    # Already ISO-8601 with Z or offset
    assert safe_iso("2026-08-19T12:00:00Z") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19T12:00:00+00:00") == "2026-08-19T12:00:00+00:00"


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


def test_parse_window_returns_utc_pair_for_a_valid_window():
    st, et = parse_window("2026-08-05T10:00:00Z", "2026-08-05T12:30:00+00:00")
    assert (st, et) == (datetime(2026, 8, 5, 10, tzinfo=UTC), datetime(2026, 8, 5, 12, 30, tzinfo=UTC))
    assert parse_window("2026-08-05", "2026-08-06") == (
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", None),
        ("", ""),
        ("garbage", "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", "garbage"),
        ("2026-08-05T12:00:00Z", "2026-08-05T12:00:00Z"),
        ("2026-08-05T12:00:00Z", "2026-08-05T10:00:00Z"),
    ],
)
def test_parse_window_is_none_for_missing_unparseable_or_non_positive_windows(start, end):
    assert parse_window(start, end) is None


# --- B5: partition-key / UTC-normalisation helpers --------------------------

_HOUR_KEYS = ["2026-10-19-00", "2026-10-19-23", "2026-02-28-23", "2024-02-29-12", "2026-1-5-3"]
_BAD_KEYS = ["", "2026-10-19", "2026-10-19-24", "2026-13-01-00", "garbage", "2026-10-19T05", "2026-10-19-05-00"]


@pytest.mark.parametrize("key", _HOUR_KEYS)
def test_parse_hour_key_matches_raw_strptime(key):
    raw = datetime.strptime(key, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    assert parse_hour_key(key) == raw
    assert parse_hour_key_strict(key) == raw
    assert parse_hour_key(key).tzinfo is UTC


@pytest.mark.parametrize("key", _BAD_KEYS)
def test_parse_hour_key_malformed(key):
    assert parse_hour_key(key) is None
    with pytest.raises(ValueError):
        parse_hour_key_strict(key)
    assert parse_hour_token(key) is None


@pytest.mark.parametrize("key", ["2026-10-19", "2024-02-29", "2026-1-5"])
def test_parse_day_key_matches_raw_strptime(key):
    raw = datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=UTC)
    assert parse_day_key(key) == raw
    assert parse_day_key_strict(key) == raw


@pytest.mark.parametrize("key", ["", "2026-02-30", "2026-10-19-05", "x"])
def test_parse_day_key_malformed(key):
    assert parse_day_key(key) is None
    with pytest.raises(ValueError):
        parse_day_key_strict(key)


def test_floor_helpers():
    dt = datetime(2026, 10, 19, 13, 45, 12, 999, tzinfo=UTC)
    assert floor_to_hour(dt) == datetime(2026, 10, 19, 13, tzinfo=UTC)
    assert floor_to_day(dt) == datetime(2026, 10, 19, tzinfo=UTC)
    naive = datetime(2026, 10, 19, 13, 45)
    assert floor_to_hour(naive) == datetime(2026, 10, 19, 13)
    assert floor_to_hour(naive).tzinfo is None


def test_ensure_utc_naive_is_labelled_utc():
    assert ensure_utc(datetime(2026, 10, 19, 5)) == datetime(2026, 10, 19, 5, tzinfo=UTC)


def test_ensure_utc_aware_preserves_instant():
    plus5 = timezone(timedelta(hours=5))
    src = datetime(2026, 10, 19, 5, tzinfo=plus5)
    out = ensure_utc(src)
    assert out == src
    assert out.utcoffset() == timedelta(0)
    assert out.hour == 0
