"""Pre-aggregated dimension counts for the high-scale serving aggregate tables.

Pure computation only. Populating request_aggregates/rum_vitals_aggregates/
rum_error_aggregates/cmcd_aggregates from these counts is
:class:`AggregateBatchAdapter`'s job; this module never touches ClickHouse.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

# One dimension per event field actually read by AggregateStore's bounded
# heavy-hitter tracking (aggregates.py) — kept in sync deliberately, since
# the reader side (aggregate_query.py) only ever asks for these by default.
DIMENSIONS_BY_DOMAIN: dict[str, tuple[str, ...]] = {
    "request": ("url", "country", "client_ip"),
    "rum_vitals": ("metric_name",),
    "rum_errors": ("error_message",),
    "cmcd": ("cmcd_session",),
}
# Only cmcd's dimension name differs from its source event field (a session
# id stored under the short key "sid" in the raw beacon).
_SOURCE_FIELD_OVERRIDES: dict[str, str] = {"cmcd_session": "sid"}
_BUCKET_SECONDS = 60


def compute_dimension_counts(
    domain: str,
    rows: tuple[Mapping[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    """Count occurrences of each configured dimension's value, per minute
    bucket. Rows without a parseable ``timestamp`` are skipped — an
    aggregate row with no time bucket to file under isn't recoverable
    information, unlike a raw fact row where the archive is authoritative
    regardless."""
    if domain not in DIMENSIONS_BY_DOMAIN:
        raise ValueError(f"unsupported aggregate write domain: {domain}")
    dimensions = DIMENSIONS_BY_DOMAIN[domain]
    counts: Counter[tuple[str, str, datetime]] = Counter()
    for row in rows:
        bucket = _bucket(row)
        if bucket is None:
            continue
        for dimension in dimensions:
            field = _SOURCE_FIELD_OVERRIDES.get(dimension, dimension)
            value = str(row.get(field, ""))
            counts[(dimension, value, bucket)] += 1
    return tuple(
        {"dimension": dimension, "value": value, "bucket_start": bucket.isoformat(), "count": count}
        for (dimension, value, bucket), count in counts.items()
    )


def _bucket(row: Mapping[str, Any]) -> datetime | None:
    value = row.get("timestamp")
    if isinstance(value, datetime):
        timestamp = value.astimezone(UTC)
    elif isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    else:
        return None
    epoch_seconds = int(timestamp.timestamp())
    return datetime.fromtimestamp(epoch_seconds - epoch_seconds % _BUCKET_SECONDS, tz=UTC)
