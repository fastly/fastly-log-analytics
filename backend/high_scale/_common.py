"""Shared ClickHouse SQL and formatting helpers for high-scale reader modules."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale._projection_utils import range_value
from backend.utils.date_utils import safe_iso

_DOMAIN_IDENT_RE = re.compile(r"^[a-z0-9_]+$")

VISIBLE_PUBLICATION_FENCE_SQL = (
    "publication_state='visible' "
    "AND batch_id IN ("
    "SELECT batch_id FROM high_scale_batch_publications FINAL "
    "WHERE service_id={service_id:String} AND publication_state='visible')"
)


def visible_publication_fence_sql(domain: str | None = None) -> str:
    """Return the publication-fence SQL predicate, optionally scoped to ``domain``."""
    if domain is None:
        return VISIBLE_PUBLICATION_FENCE_SQL
    if not _DOMAIN_IDENT_RE.fullmatch(domain):
        raise ValueError(f"Invalid publication domain identifier: {domain!r}")
    return (
        "publication_state='visible' "
        "AND batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' "
        "AND publication_state='visible')"
    )


def visible_fact_clauses(domain: str) -> list[str]:
    """Predicates (with a ``{service_id:String}`` parameter) limiting a fact table to visible batches of ``domain``."""
    if not _DOMAIN_IDENT_RE.fullmatch(domain):
        raise ValueError(f"Invalid publication domain identifier: {domain!r}")
    return [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' "
        "AND publication_state='visible')",
    ]


def parse_time_window(
    start_time: str | None,
    end_time: str | None,
    default: timedelta = timedelta(days=1),
    *,
    utc: bool = False,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Parse ``(start_time, end_time)`` ISO bounds, defaulting ``end`` to ``now`` and ``start`` to ``end - default``."""
    end = range_value(end_time) if end_time else (now or datetime.now(UTC))
    if utc:
        end = end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC)
    start = range_value(start_time) if start_time else (end - default)
    if utc:
        start = start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC)
    return start, end


def inclusive_minute_end(end: datetime) -> datetime:
    """Extend a whole-minute upper bound by 1 minute so ``event_timestamp < end_upper`` includes the minute."""
    return end + timedelta(minutes=1) if end.second == 0 and end.microsecond == 0 else end


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
