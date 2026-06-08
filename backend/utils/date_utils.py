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
    return str(dt)


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
