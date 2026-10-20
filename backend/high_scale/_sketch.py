"""Mergeable log-histogram sketch for high-scale latency percentiles.

Percentiles cannot be combined across minute rows (a request-weighted average of
per-minute p95s is not the window p95), so projection rows persist a compact
histogram instead: parallel ``values`` / ``counts`` arrays. Values are snapped to
the representative centre of a geometric bucket of ratio ``GAMMA``, which bounds
the relative error of any quantile read back from the merged histogram by
``RELATIVE_ERROR`` (the DDSketch guarantee). ClickHouse merges the arrays exactly
at read time with ``quantilesExactWeightedArray(values, counts)``.
"""

from __future__ import annotations

import math
from collections import Counter

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
