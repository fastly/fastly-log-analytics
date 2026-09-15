"""Deterministic release timing helpers for synthetic load generators."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class ReleaseReport:
    scheduled_release_at: datetime
    first_upload_at: datetime | None
    last_upload_at: datetime | None
    bytes_uploaded: int
    lines: int
    wall_seconds: float
    files: int
    errors: int


def release_schedule(
    *,
    target_rps: int,
    period_seconds: int,
    duration_seconds: int,
    started_at: datetime,
    release_interval_seconds: int | None = None,
) -> tuple[datetime, ...]:
    """Return one release timestamp per complete generated period."""
    if target_rps <= 0:
        raise ValueError("target_rps must be positive")
    if period_seconds <= 0:
        raise ValueError("period_seconds must be positive")
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    interval = release_interval_seconds if release_interval_seconds is not None else period_seconds
    if interval <= 0:
        raise ValueError("release interval must be positive")

    periods = max(1, duration_seconds // period_seconds)
    return tuple(started_at + timedelta(seconds=i * interval) for i in range(periods))
