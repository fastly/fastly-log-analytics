"""Shared ClickHouse SQL and formatting helpers for high-scale reader modules."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from backend.utils.date_utils import safe_iso

_TIMESERIES_METRICS: dict[tuple[str, str], tuple[str, float]] = {
    ("ttfb", "p50"): ("latency_us", 0.5),
    ("ttfb", "p95"): ("latency_us", 0.95),
    ("ttfb", "p99"): ("latency_us", 0.99),
    ("ttlb", "p50"): ("ottlb_us", 0.5),
    ("ttlb", "p95"): ("ottlb_us", 0.95),
    ("ttlb", "p99"): ("ottlb_us", 0.99),
}


def format_iso_utc(value: Any) -> str | None:
    """Format a datetime or timestamp value as an ISO-8601 UTC ``...Z`` string."""
    if isinstance(value, datetime):
        stamp = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return stamp.isoformat().replace("+00:00", "Z")
    return safe_iso(value)


def format_iso_utc_or_empty(value: Any) -> str:
    """Format a datetime or timestamp value as an ISO-8601 UTC ``...Z`` string (or ``""``)."""
    return format_iso_utc(value) or ""


def _bucket_case(column: str, buckets: tuple[tuple[int, str], ...], last: str) -> str:
    """Build a ClickHouse ``multiIf(col <= limit, 'label', ..., 'last')`` expression."""
    arms = ", ".join(f"{column} <= {limit}, '{label}'" for limit, label in buckets)
    return f"multiIf({arms}, '{last}')"
