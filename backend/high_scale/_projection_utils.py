"""Primitives shared by the high-scale projection modules and ClickHouse range parsing."""

from __future__ import annotations

import math
from datetime import UTC, datetime


def range_value(value: str | None) -> datetime | None:
    """Parse an ISO-8601 bound as-is: ``Z`` becomes ``+00:00``; naive input stays naive."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def bucket(value: object) -> datetime | None:
    """Floor a datetime or ISO string to its UTC minute; ``None`` when unparseable."""
    if isinstance(value, datetime):
        timestamp = value.astimezone(UTC)
    elif isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    else:
        return None
    return timestamp.replace(second=0, microsecond=0)


def text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def finite_float(value: object) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def percentile(values: list[float], percentile: float) -> float | None:
    """Linear-interpolated percentile (``percentile`` in [0, 1]) of ``values``."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction
