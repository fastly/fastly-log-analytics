"""Time-window quantization, range token resolution, and insights window selection."""

from __future__ import annotations

import hashlib
import math
from datetime import UTC, datetime, timedelta

from backend.utils.bounded_cache import CacheRegistry as CacheRegistry
from backend.utils.date_utils import iso_z, parse_iso_utc

STATIC_DEFAULT: tuple[float, float] = (24.0, 168.0)
_BANDS: tuple[tuple[float, float, float], ...] = (
    (1.0, 0.25, 1.0),
    (4.0, 1.0, 1.0),
    (24.0, 4.0, 1.0),
    (48.0, 4.0, 24.0),
    (168.0, 24.0, 24.0),
    (math.inf, 24.0, 168.0),
)
ANCHOR_QUANTUM_SECONDS = 60
_FIXED_TOKEN_DELTAS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "3h": timedelta(hours=3),
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "24h": timedelta(hours=24),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}
VALID_RANGE_TOKENS: frozenset[str] = frozenset(_FIXED_TOKEN_DELTAS) | {"auto"}


def history_hours_from_earliest(earliest: str | None, *, now: datetime | None = None) -> float | None:
    """Compute elapsed hours between ``earliest`` ISO timestamp and ``now``."""
    dt = parse_iso_utc(earliest)
    if dt is None:
        return None
    anchor = now if now is not None else datetime.now(UTC)
    return max(0.0, (anchor - dt).total_seconds() / 3600.0)


def pick_insights_default(history_hours: float | None) -> tuple[float, float]:
    """Select ``(window_hours, baseline_hours)`` for insights based on available history."""
    if history_hours is None or not math.isfinite(history_hours):
        return STATIC_DEFAULT
    for upper, window, baseline in _BANDS:
        if history_hours < upper:
            return (window, baseline)
    return STATIC_DEFAULT


def is_valid_range_token(token: str | None) -> bool:
    """Return whether ``token`` is a supported dashboard range token."""
    return token in VALID_RANGE_TOKENS


def quantize_anchor(anchor: str | None, *, now: datetime | None = None) -> str:
    """Floor ``anchor`` (or ``now``) to the nearest ``ANCHOR_QUANTUM_SECONDS`` boundary."""
    parsed = parse_iso_utc(anchor) if anchor else None
    if parsed is not None:
        dt = parsed
    elif now is not None:
        dt = now
    else:
        dt = datetime.now(UTC)
    epoch = int(dt.timestamp())
    quantized_epoch = epoch - (epoch % ANCHOR_QUANTUM_SECONDS)
    return iso_z(datetime.fromtimestamp(quantized_epoch, tz=UTC))


def _pick_auto_token(earliest_log_at: str | None, *, now: datetime) -> str:
    history_hours = history_hours_from_earliest(earliest_log_at, now=now)
    if history_hours is None:
        return "7d"
    if history_hours < 7 * 24:
        return "24h"
    if history_hours < 30 * 24:
        return "7d"
    return "30d"


def resolve_window(
    range_token: str | None,
    anchor: str | None,
    *,
    earliest_log_at: str | None = None,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Resolve a ``range_token`` and ``anchor`` into quantized ``(start_iso, end_iso)`` UTC strings."""
    if not is_valid_range_token(range_token) or range_token is None:
        raise ValueError(f"unrecognized range_token: {range_token!r}")
    now_dt = now if now is not None else datetime.now(UTC)
    anchor_dt = parse_iso_utc(quantize_anchor(anchor, now=now_dt))
    if anchor_dt is None:
        raise RuntimeError("anchor_dt is unexpectedly None")
    token = _pick_auto_token(earliest_log_at, now=now_dt) if range_token == "auto" else range_token
    return iso_z(anchor_dt - _FIXED_TOKEN_DELTAS[token]), iso_z(anchor_dt)


def invite_clamp_fingerprint(analyst_session: object | None) -> str | None:
    """Return a short hash fingerprint of an analyst session's time-window clamp settings."""
    if analyst_session is None:
        return None
    qs = getattr(analyst_session, "query_start_time", None)
    qe = getattr(analyst_session, "query_end_time", None)
    qw = getattr(analyst_session, "query_window_hours", None)
    raw = f"{qs or ''}|{qe or ''}|{qw or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


__all__ = [
    "ANCHOR_QUANTUM_SECONDS",
    "STATIC_DEFAULT",
    "VALID_RANGE_TOKENS",
    "_BANDS",
    "_FIXED_TOKEN_DELTAS",
    "_pick_auto_token",
    "history_hours_from_earliest",
    "invite_clamp_fingerprint",
    "is_valid_range_token",
    "pick_insights_default",
    "quantize_anchor",
    "resolve_window",
]
