"""Centralized UTC date/time parsing and formatting utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


def parse_iso_utc(s: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp or YYYY-MM-DD date string into a timezone-aware UTC datetime."""
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d.astimezone(UTC) if d.tzinfo is not None else d.replace(tzinfo=UTC)
    except ValueError:
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            return None


def parse_window(start: str | None, end: str | None) -> tuple[datetime, datetime] | None:
    """Parse ``(start, end)`` ISO strings into UTC datetimes if both are valid and ``end > start``."""
    st = parse_iso_utc(start)
    et = parse_iso_utc(end)
    if st is None or et is None or et <= st:
        return None
    return st, et


def parse_hour_key_strict(s: str) -> datetime:
    """Parse a ``YYYY-MM-DD-HH`` hour partition key into a UTC datetime or raise ``ValueError``."""
    return datetime.strptime(s, "%Y-%m-%d-%H").replace(tzinfo=UTC)


def parse_hour_key(s: str) -> datetime | None:
    """Parse a ``YYYY-MM-DD-HH`` hour partition key into a UTC datetime, returning ``None`` on failure."""
    try:
        return parse_hour_key_strict(s)
    except ValueError:
        return None


def parse_day_key_strict(s: str) -> datetime:
    """Parse a ``YYYY-MM-DD`` day partition key into a UTC datetime or raise ``ValueError``."""
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)


def ensure_utc(dt: datetime) -> datetime:
    """Return ``dt`` as a timezone-aware UTC datetime."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def iso_z(dt: datetime) -> str:
    """Format ``dt`` as ``YYYY-MM-DDTHH:MM:SSZ`` in UTC."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_z_now() -> str:
    """Return the current UTC timestamp formatted as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return iso_z(datetime.now(UTC))


def _parse_dt(s: str, default: datetime) -> datetime:
    dt = parse_iso_utc(s)
    return dt if dt is not None else default


def parse_date_window(start: str, end: str, default_days: int = 7) -> tuple[str, str]:
    """Normalize ``(start, end)`` strings into UTC ``YYYY-MM-DDTHH:MM:SSZ`` bounds."""
    now = datetime.now(UTC)
    start_dt = _parse_dt(start, now - timedelta(days=default_days))
    end_dt = _parse_dt(end, now)
    if len(end) <= 10:
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
    return iso_z(start_dt), iso_z(end_dt)


def safe_iso(dt: Any) -> str | None:
    """Convert a datetime or timestamp string into a trailing-``Z`` ISO-8601 string."""
    if dt is None:
        return None
    if hasattr(dt, "isoformat"):
        s = dt.isoformat()
        if not s.endswith("Z") and "+" not in s and s.count("-") <= 2:
            s += "Z"
        return s
    s = str(dt).strip()
    if not s:
        return None
    if " " in s:
        s = s.replace(" ", "T")
        if not s.endswith("Z") and "+" not in s:
            s += "Z"
    elif "T" in s and not s.endswith("Z") and "+" not in s:
        s += "Z"
    return s


def parse_window_str_to_dt(s: str) -> datetime:
    """Parse a date window string into a UTC datetime or raise ``ValueError``."""
    dt = parse_iso_utc(s)
    if dt is None:
        raise ValueError(f"Invalid date window string: {s}")
    return dt


def window_to_epoch(start: str, end: str) -> tuple[str, str, int, int]:
    """Normalize ``(start, end)`` and return ``(start_iso, end_iso, start_epoch, end_epoch)``."""
    s, e = parse_date_window(start, end)
    start_epoch = int(parse_window_str_to_dt(s).timestamp())
    end_epoch = int(parse_window_str_to_dt(e).timestamp())
    return s, e, start_epoch, end_epoch


def parse_relative_time_window(since: str, max_lookback_days: int = 30) -> datetime:
    """Resolve a relative lookback token (e.g. ``1h``, ``7d``, ``30m``) to a UTC cutoff datetime."""
    now = datetime.now(UTC)
    if not since:
        return now - timedelta(hours=1)
    s = since.strip().lower()
    try:
        if s.endswith("h"):
            hours = max(1, min(int(s[:-1]), max_lookback_days * 24))
            return now - timedelta(hours=hours)
        if s.endswith("d"):
            days = max(1, min(int(s[:-1]), max_lookback_days))
            return now - timedelta(days=days)
        if s.endswith("m"):
            minutes = max(1, min(int(s[:-1]), max_lookback_days * 24 * 60))
            return now - timedelta(minutes=minutes)
    except ValueError:
        pass
    return now - timedelta(hours=1)


__all__ = [
    "_parse_dt",
    "ensure_utc",
    "iso_z",
    "iso_z_now",
    "parse_date_window",
    "parse_day_key_strict",
    "parse_hour_key",
    "parse_hour_key_strict",
    "parse_iso_utc",
    "parse_relative_time_window",
    "parse_window",
    "parse_window_str_to_dt",
    "safe_iso",
    "window_to_epoch",
]
