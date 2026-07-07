"""Backend mirror of the frontend's adaptive Insights default picker.

PARITY CONTRACT (load-bearing): ``frontend/lib/insights-defaults.ts``
(``historyHoursFromExtents`` + ``pickInsightsDefault``) decides which
window/baseline pair the Insights page requests by default, based on how
much log history the active service has. The ``/api/insights`` cache key
folds in both hour values (``backend/repositories/insights/repository.py``),
so the prewarmer must warm EXACTLY the pair the page will pick — warming any
other pair is a guaranteed cache miss for every default page load. That is
the regression this module fixes: once a service accrues ≥720 h (30 d) of
history the page defaults to (1 h, 720 h), while the prewarmer warmed only
the static (1 h, 168 h), so the default selection cold-computed (~20 s on a
long-history service) on every visit outside the cache TTL.

Both roles resolve the same bucket: the extents the client picks from
(bootstrap ``log_extents`` / ``/api/log-extents``) are the service-wide
snapshot, un-clamped for analysts, so one derivation per service covers the
admin warm and every analyst-shape warm.

Keep the band table here identical to the one in
``frontend/lib/insights-defaults.ts``. ``tests/utils/test_insights_defaults.py``
pins every band AND parses the TS source so the two sides cannot drift
silently.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime

from backend.utils.date_utils import parse_iso_utc

# The historical static default (also the InsightsRequest field defaults):
# recent 1 h vs previous 7 d. Used when a service has no extents yet, so
# no-data services behave exactly as before.
STATIC_DEFAULT: tuple[float, float] = (1.0, 168.0)

# (history_upper_bound_hours, window_hours, baseline_hours). Half-open bands
# over available history — an exact boundary selects the higher bucket, and
# the final +inf band is the ≥30 d shape. Values are floats because the
# client sends ``parseFloat`` of the dropdown tokens, so these produce
# byte-identical cache-key fragments.
_BANDS: tuple[tuple[float, float, float], ...] = (
    (1.0, 0.25, 1.0),
    (4.0, 1.0, 1.0),
    (24.0, 4.0, 1.0),
    (48.0, 4.0, 24.0),
    (168.0, 24.0, 24.0),
    (720.0, 1.0, 168.0),
    (math.inf, 1.0, 720.0),
)


def history_hours_from_earliest(earliest: str | None, *, now: datetime | None = None) -> float | None:
    """Hours of history available = ``now − earliest_log_at``, floored at 0.

    Mirrors ``historyHoursFromExtents``: ``parse_iso_utc`` widens a date-only
    extent ("2026-06-15") to UTC start-of-day the same way the TS side does;
    absent/unparseable extents return None (→ ``STATIC_DEFAULT``).
    """
    dt = parse_iso_utc(earliest)
    if dt is None:
        return None
    anchor = now if now is not None else datetime.now(UTC)
    return max(0.0, (anchor - dt).total_seconds() / 3600.0)


def pick_insights_default(history_hours: float | None) -> tuple[float, float]:
    """Map available history to the default ``(window_hours, baseline_hours)``."""
    if history_hours is None or not math.isfinite(history_hours):
        return STATIC_DEFAULT
    for upper, window, baseline in _BANDS:
        if history_hours < upper:
            return (window, baseline)
    return STATIC_DEFAULT  # unreachable: the last band's bound is +inf
