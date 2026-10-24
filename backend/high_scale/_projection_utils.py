"""Primitives shared by the high-scale projection modules and ClickHouse range parsing."""

from __future__ import annotations

import math
from collections import Counter
from datetime import UTC, datetime
from typing import overload

GAMMA = 1.02
RELATIVE_ERROR = (GAMMA - 1.0) / (GAMMA + 1.0)
_LOG_GAMMA = math.log(GAMMA)


def _centroid(value: float) -> float:
    if value == 0.0:
        return 0.0
    magnitude = abs(value)
    index = math.ceil(math.log(magnitude) / _LOG_GAMMA)
    centre = 2.0 * GAMMA**index / (GAMMA + 1.0)
    return math.copysign(centre, value)


def sketch(values: list[float]) -> tuple[list[float], list[int]]:
    """Return ``(centroids, counts)`` sorted by centroid; every observation is counted once."""
    histogram = Counter(_centroid(value) for value in values)
    centroids = sorted(histogram)
    return centroids, [histogram[centroid] for centroid in centroids]


@overload
def range_value(value: str) -> datetime: ...


@overload
def range_value(value: None) -> None: ...


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


def optional_int(value: object) -> int | None:
    parsed = finite_float(value)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def percentiles(values: list[float], qs: tuple[float, ...]) -> list[float | None]:
    """Linear-interpolated percentiles (each ``q`` in [0, 1]) from a single sort of ``values``."""
    if not values:
        return [None] * len(qs)
    ordered = sorted(values)
    out: list[float | None] = []
    for q in qs:
        position = (len(ordered) - 1) * q
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            out.append(ordered[lower])
        else:
            out.append(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower))
    return out
