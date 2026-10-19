"""Date helpers for rollup tests.

They read ``datetime.now(UTC)`` at call time, so a test that requests the ``frozen_clock``
fixture gets deterministic dates (no midnight / hour-boundary flake window).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def days_ago_iso(days: int) -> str:
    return (datetime.now(UTC).date() - timedelta(days=days)).strftime("%Y-%m-%d")


def yesterday_iso() -> str:
    return days_ago_iso(1)


def two_days_ago_iso() -> str:
    return days_ago_iso(2)


def three_days_ago_iso() -> str:
    return days_ago_iso(3)


def past_hour(hours_ago: int) -> tuple[str, datetime]:
    """``(partition key "YYYY-MM-DD-HH", hour-aligned datetime)`` for ``hours_ago`` hours back."""
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt
