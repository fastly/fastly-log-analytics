"""Time-bucket alignment, zero-filling, time-series normalization, and timestamp formatting helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from backend.utils.date_utils import ensure_utc, parse_iso_utc

CHART_INTERVAL_SECONDS: dict[str, int] = {
    "1 second": 1,
    "5 seconds": 5,
    "10 seconds": 10,
    "15 seconds": 15,
    "30 seconds": 30,
    "1 minute": 60,
    "5 minutes": 300,
    "10 minutes": 600,
    "15 minutes": 900,
    "30 minutes": 1800,
    "1 hour": 3600,
    "1 day": 86400,
}

_CHART_INTERVAL_ALIASES: dict[str, str] = {
    "1 second": "1 second",
    "1 seconds": "1 second",
    "1s": "1 second",
    "second": "1 second",
    "seconds": "1 second",
    "5 seconds": "5 seconds",
    "5s": "5 seconds",
    "10 seconds": "10 seconds",
    "10s": "10 seconds",
    "15 seconds": "15 seconds",
    "15s": "15 seconds",
    "30 seconds": "30 seconds",
    "30s": "30 seconds",
    "1 minute": "1 minute",
    "1 minutes": "1 minute",
    "1m": "1 minute",
    "minute": "1 minute",
    "minutes": "1 minute",
    "5 minute": "5 minutes",
    "5 minutes": "5 minutes",
    "5m": "5 minutes",
    "10 minutes": "10 minutes",
    "10m": "10 minutes",
    "15 minutes": "15 minutes",
    "15m": "15 minutes",
    "30 minutes": "30 minutes",
    "30m": "30 minutes",
    "1 hour": "1 hour",
    "1 hours": "1 hour",
    "1h": "1 hour",
    "hour": "1 hour",
    "hours": "1 hour",
    "1 day": "1 day",
    "1 days": "1 day",
    "1d": "1 day",
    "day": "1 day",
    "days": "1 day",
}

CMCD_TS_DEFAULTS: dict[str, dict[str, Any]] = {
    "sessions_ts": {"concurrent_sessions": 0, "rebuffer_session_pct": None, "new_sessions": 0},
    "buffer_health_ts": {"p50_buffer": None, "p95_buffer": None, "starvation_rate": None},
    "bitrate_ts": {"avg_bitrate": None, "utilization_ratio": None},
    "throughput_ts": {"p50": None, "p95": None, "p99": None},
    "startup_ts": {"startup_ratio": None},
}


def normalize_chart_interval(chart_interval: str | None, default: str = "1 minute") -> str:
    """Validate and normalize a dashboard chart interval string."""
    if not chart_interval:
        return default
    ci = chart_interval.strip().lower()
    if ci in _CHART_INTERVAL_ALIASES:
        return _CHART_INTERVAL_ALIASES[ci]
    return ci if ci in CHART_INTERVAL_SECONDS else default


def chart_interval_seconds(chart_interval: str | None, default: int = 60) -> int:
    """Return the duration in seconds for a chart interval string."""
    return CHART_INTERVAL_SECONDS.get(normalize_chart_interval(chart_interval), default)


def _coerce_utc_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_utc(value)
    return parse_iso_utc(value)


def clamp_bucket_seconds(
    bucket_seconds: int,
    start_time: str | datetime | None,
    end_time: str | datetime | None,
    *,
    max_buckets: int = 8640,
) -> int:
    """Floor ``bucket_seconds`` so the time span contains at most ``max_buckets`` buckets."""
    st = _coerce_utc_datetime(start_time)
    et = _coerce_utc_datetime(end_time)
    if st is None or et is None:
        return bucket_seconds
    span_secs = max(1, int((et - st).total_seconds()))
    return max(bucket_seconds, span_secs // max_buckets) or bucket_seconds


def generate_epoch_aligned_buckets(
    start_time: str | datetime | None,
    end_time: str | datetime | None,
    bucket_seconds: int,
    *,
    fmt: str = "%Y-%m-%dT%H:%M:%S",
) -> list[str]:
    """Generate epoch-aligned bucket timestamp strings covering ``[start_time, end_time]``."""
    st = _coerce_utc_datetime(start_time)
    et = _coerce_utc_datetime(end_time)
    if st is None or et is None or bucket_seconds <= 0:
        return []
    first = (int(st.timestamp()) // bucket_seconds) * bucket_seconds
    last = (int(et.timestamp()) // bucket_seconds) * bucket_seconds
    if last < first:
        return []
    return [datetime.fromtimestamp(ts, tz=UTC).strftime(fmt) for ts in range(first, last + 1, bucket_seconds)]


def pad_timeseries_buckets(
    start_time: str | datetime | None,
    end_time: str | datetime | None,
    bucket_seconds: int,
    rows_by_bucket: Mapping[str, Mapping[str, Any]],
    default_vals: Mapping[str, Any],
    *,
    bucket_fmt: str = "%Y-%m-%d %H:%M:%S",
) -> list[dict[str, Any]]:
    """Zero-fill missing time-series buckets between ``start_time`` and ``end_time``.

    Accepts ``start_time`` / ``end_time`` as ISO strings, ``datetime`` objects, or ``None``.
    Falls back to sorted existing buckets when bounds cannot be parsed.
    """
    st = _coerce_utc_datetime(start_time)
    et = _coerce_utc_datetime(end_time)
    if st is None or et is None or bucket_seconds <= 0:
        return sorted([{"bucket": k, **v} for k, v in rows_by_bucket.items()], key=lambda x: x["bucket"])

    st_epoch = int(st.timestamp())
    st_aligned = st_epoch - (st_epoch % bucket_seconds)

    et_epoch = int(et.timestamp())
    et_aligned = et_epoch - (et_epoch % bucket_seconds)
    if et_aligned < st_aligned:
        return []

    has_callables = any(callable(v) for v in default_vals.values())
    static_defaults = None if has_callables else dict(default_vals)

    padded: list[dict[str, Any]] = []
    for ts in range(st_aligned, et_aligned + 1, bucket_seconds):
        bucket_str = datetime.fromtimestamp(ts, tz=UTC).strftime(bucket_fmt)
        existing = rows_by_bucket.get(bucket_str)
        if existing is not None:
            padded.append({"bucket": bucket_str, **existing})
        elif static_defaults is not None:
            padded.append({"bucket": bucket_str, **static_defaults})
        else:
            padded.append({"bucket": bucket_str, **{k: (v() if callable(v) else v) for k, v in default_vals.items()}})
    return padded


INSIGHT_BUCKET_INTERVALS: tuple[tuple[float, str], ...] = (
    (2, "1 minute"),
    (12, "5 minutes"),
    (48, "15 minutes"),
    (168, "1 hour"),
    (720, "4 hours"),
)


def select_insight_bucket_interval(
    total_hours: float,
    *,
    clickhouse: bool = False,
    clickhouse_syntax: bool = False,
) -> str:
    """Select the adaptive time-bucket interval for insights timeline queries."""
    use_ch = clickhouse or clickhouse_syntax
    if use_ch:
        if total_hours <= 2:
            return "INTERVAL 1 MINUTE"
        if total_hours <= 12:
            return "INTERVAL 5 MINUTE"
        if total_hours <= 48:
            return "INTERVAL 15 MINUTE"
        if total_hours <= 168:
            return "INTERVAL 1 HOUR"
        if total_hours <= 720:
            return "INTERVAL 4 HOUR"
        return "INTERVAL 1 DAY"
    return next((iv for max_h, iv in INSIGHT_BUCKET_INTERVALS if total_hours <= max_h), "1 day")
