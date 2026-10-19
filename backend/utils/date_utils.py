from datetime import UTC, datetime, timedelta


def parse_iso_utc(s: str | None) -> datetime | None:
    """Parse an ISO-8601 or YYYY-MM-DD string to a UTC datetime. Returns None if invalid or None."""
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d.astimezone(UTC) if d.tzinfo is not None else d.replace(tzinfo=UTC)
    except ValueError:
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            return None


def parse_window(start: str | None, end: str | None) -> tuple[datetime, datetime] | None:
    """Parse a ``(start, end)`` ISO pair to UTC datetimes, or ``None`` when either is missing/unparseable
    or the window is empty/inverted (``end <= start``)."""
    st = parse_iso_utc(start)
    et = parse_iso_utc(end)
    if st is None or et is None or et <= st:
        return None
    return st, et


def parse_hour_key_strict(s: str) -> datetime:
    """Parse a ``YYYY-MM-DD-HH`` partition key to a tz-aware UTC datetime.

    Raises ``ValueError`` on malformed input (same contract as the raw
    ``datetime.strptime`` it replaces).
    """
    return datetime.strptime(s, "%Y-%m-%d-%H").replace(tzinfo=UTC)


def parse_hour_key(s: str) -> datetime | None:
    """Like :func:`parse_hour_key_strict` but returns ``None`` when malformed."""
    try:
        return parse_hour_key_strict(s)
    except ValueError:
        return None


def parse_day_key_strict(s: str) -> datetime:
    """Parse a ``YYYY-MM-DD`` partition key to midnight UTC (raises ``ValueError``)."""
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)


def parse_day_key(s: str) -> datetime | None:
    """Like :func:`parse_day_key_strict` but returns ``None`` when malformed."""
    try:
        return parse_day_key_strict(s)
    except ValueError:
        return None


def floor_to_hour(dt: datetime) -> datetime:
    """Truncate to the top of the hour, preserving tzinfo."""
    return dt.replace(minute=0, second=0, microsecond=0)


def floor_to_day(dt: datetime) -> datetime:
    """Truncate to midnight, preserving tzinfo."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def ensure_utc(dt: datetime) -> datetime:
    """Return ``dt`` as an aware UTC datetime.

    Naive values are treated as UTC (DuckDB TIMESTAMP is naive-UTC); aware
    values are converted with ``astimezone`` so the instant is preserved.
    """
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def iso_z(dt: datetime) -> str:
    """Format a datetime to ISO 8601 with a Z suffix."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_z_now() -> str:
    """Return the current UTC time as an ISO 8601 string with a Z suffix."""
    return iso_z(datetime.now(UTC))


def _parse_dt(s: str, default: datetime) -> datetime:
    """Parse an ISO-8601 or YYYY-MM-DD string to a UTC datetime, returning default on failure."""
    dt = parse_iso_utc(s)
    return dt if dt is not None else default


def parse_date_window(start: str, end: str, default_days: int = 7) -> tuple[str, str]:
    now = datetime.now(UTC)
    start_dt = _parse_dt(start, now - timedelta(days=default_days))
    end_dt = _parse_dt(end, now)

    if len(end) <= 10:
        end_dt = end_dt.replace(hour=23, minute=59, second=59)

    return iso_z(start_dt), iso_z(end_dt)


def safe_iso(dt) -> str | None:
    """Normalise a datetime or string to an ISO-8601 string ending in Z.

    DuckDB TIMESTAMP is timezone-naive but always represents UTC; appending
    Z ensures JavaScript parses it as UTC instead of local time. Used by
    both the duckdb core layer and the repositories layer.
    """
    if dt is None:
        return None
    if hasattr(dt, "isoformat"):
        s = dt.isoformat()
        if not s.endswith("Z") and "+" not in s and s.count("-") <= 2:
            s += "Z"
        return s
    s = str(dt).strip()
    if not s:
        return None
    if " " in s:
        s = s.replace(" ", "T")
        if not s.endswith("Z") and "+" not in s:
            s += "Z"
    elif "T" in s and not s.endswith("Z") and "+" not in s:
        s += "Z"
    return s


def parse_window_str_to_dt(s: str) -> datetime:
    """Parse a string returned by ``parse_date_window`` back into a UTC datetime.

    ``parse_date_window`` returns ISO-8601 ``"%Y-%m-%dT%H:%M:%SZ"``; callers
    that need a ``datetime`` for arithmetic (e.g. ``.timestamp()``) should
    use this helper rather than ``strptime("%Y-%m-%d %H:%M:%S")``, which
    fails on the T separator and Z suffix.
    """
    dt = parse_iso_utc(s)
    if dt is None:
        raise ValueError(f"Invalid date window string: {s}")
    return dt


def window_to_epoch(start: str, end: str) -> tuple[str, str, int, int]:
    """Parse a (start, end) request window into its ISO-Z strings plus the
    matching epoch-second integers.

    Folds the three-step ``parse_date_window`` → ``parse_window_str_to_dt``
    → ``int(.timestamp())`` dance that the usage router's Fastly /stats
    handlers each repeated. Returns ``(start_str, end_str, from_ts, to_ts)``.
    """
    s, e = parse_date_window(start, end)
    return s, e, int(parse_window_str_to_dt(s).timestamp()), int(parse_window_str_to_dt(e).timestamp())


def parse_relative_time_window(since: str, max_lookback_days: int = 30) -> datetime:
    """Parse a window string like ``"1h"`` / ``"24h"`` / ``"7d"`` / ``"30m"``
    into a UTC datetime ``now - delta``.

    Capped at ``max_lookback_days`` so an oversized input can't pull data
    beyond the retention window. Falls back to ``now - 1 hour`` for empty /
    malformed input — admin UIs that drive this pass a controlled set of
    values, so silent fallback is preferable to surfacing a 400.
    """
    now = datetime.now(UTC)
    if not since:
        return now - timedelta(hours=1)
    s = since.strip().lower()
    try:
        if s.endswith("h"):
            hours = max(1, min(int(s[:-1]), max_lookback_days * 24))
            return now - timedelta(hours=hours)
        if s.endswith("d"):
            days = max(1, min(int(s[:-1]), max_lookback_days))
            return now - timedelta(days=days)
        if s.endswith("m"):
            minutes = max(1, min(int(s[:-1]), max_lookback_days * 24 * 60))
            return now - timedelta(minutes=minutes)
    except ValueError:
        pass
    return now - timedelta(hours=1)
