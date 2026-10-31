"""Bounded retained-minute interiors and verified raw edges for exact readers."""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from backend.core.rollups._common import (
    DAY_BUNDLE_FILENAME,
    OVERVIEW_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    _day_bundled_root,
    _day_rollups_root,
    _hour_bundled_root,
    quote_path_list,
)
from backend.core.rollups.rollup_coverage import (
    MISSING_HOUR_HEAL_CAP,
    BundleProbe,
    CoverageIndex,
    _day_bundle_file,
    cached_listdir,
    validated_daily_reference_minutes,
)
from backend.utils.date_utils import parse_day_key_strict, parse_hour_key_strict, parse_iso_utc
from backend.utils.telemetry import log_once_per

Range = tuple[datetime, datetime, bool]
Proof = tuple[datetime, datetime, dict[datetime, int]]
Generation = tuple[int, ...]
_logger = logging.getLogger("backend.repositories._base")


class HistoricalCoverageUnavailable(RuntimeError):
    """Do not convert an incomplete retained-history read into a raw fallback."""


class Reader(Protocol):
    src: dict[str, Any]

    def execute(self, q: str, p: list[Any] | None = None) -> Any: ...


def _drop_temp_table(executor: Any, temp_table: str | None, label: str = "temp_cleanup") -> None:
    if not temp_table:
        return
    try:
        executor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
    except Exception:
        if log_once_per(f"{label}: DROP TABLE failed (ignored)", 60):
            _logger.debug("%s: DROP TABLE failed (ignored)", label, exc_info=True)


def _unavailable(reason: str, label: str = "analytics") -> HistoricalCoverageUnavailable:
    return HistoricalCoverageUnavailable(f"Historical {label} coverage unavailable: {reason}")


def generation(path: str) -> Generation:
    st = os.stat(path)
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


def has_subminute_bounds(start_time: str | None, end_time: str | None) -> bool:
    if not start_time or not end_time:
        return False
    return any(
        dt is not None and (dt.second or dt.microsecond) for dt in (parse_iso_utc(start_time), parse_iso_utc(end_time))
    )


def reference_exists(src: dict[str, Any], hour: str) -> bool:
    return bool(
        BundleProbe(_hour_bundled_root(src)).has(hour, TIME_SERIES_BUNDLE_FILENAME)
        or _day_bundle_file(_day_bundled_root(src), hour[:10], TIME_SERIES_BUNDLE_FILENAME)
    )


def range_sql(ranges: list[Range]) -> str:
    return (
        " OR ".join(
            f"(timestamp >= TIMESTAMPTZ '{start.isoformat()}' "
            f"AND timestamp {'<=' if inclusive else '<'} TIMESTAMPTZ '{end.isoformat()}')"
            for start, end, inclusive in ranges
        )
        or "false"
    )


def merge_ranges(ranges: list[Range]) -> list[Range]:
    merged: list[Range] = []
    for start, end, inclusive in sorted(ranges):
        if merged and start <= merged[-1][1]:
            previous_start, previous_end, previous_inclusive = merged.pop()
            merged_end = max(previous_end, end)
            merged.append(
                (
                    previous_start,
                    merged_end,
                    (end == merged_end and inclusive) or (previous_end == merged_end and previous_inclusive),
                )
            )
        else:
            merged.append((start, end, inclusive))
    return merged


def minute_reference_proofs(
    reader: Reader, hours: list[str], label: str, proof_minutes: list[datetime] | None = None
) -> tuple[dict[str, Proof], dict[str, Generation]]:
    """The same canonical publication proof is used by Top-N, scalar and chart."""
    probe = BundleProbe(_hour_bundled_root(reader.src))
    day_root = _day_bundled_root(reader.src)
    expected: dict[str, Proof] = {}
    generations: dict[str, Generation] = {}
    by_path: dict[str, dict[datetime, int]] = {}
    requested_minutes: dict[str, list[datetime]] = {}
    for minute in proof_minutes or []:
        requested_minutes.setdefault(minute.strftime("%Y-%m-%d-%H"), []).append(minute)
    for hour in sorted(set(hours) | requested_minutes.keys()):
        start = parse_hour_key_strict(hour)
        hourly = probe.has(hour, TIME_SERIES_BUNDLE_FILENAME)
        path = (
            probe.path(hour, TIME_SERIES_BUNDLE_FILENAME)
            if hourly
            else _day_bundle_file(day_root, hour[:10], TIME_SERIES_BUNDLE_FILENAME)
        )
        if path is None:
            raise _unavailable("missing retained minute reference", label)
        if path not in by_path:
            generations[path] = generation(path)
            if hourly and probe.is_empty(hour, TIME_SERIES_BUNDLE_FILENAME):
                schema = {
                    row[0]: row[1]
                    for row in reader.execute(
                        f"DESCRIBE SELECT * FROM read_parquet([{quote_path_list([path])}], hive_partitioning=false)"
                    ).fetchall()
                }
                if schema.get("bucket") != "TIMESTAMP WITH TIME ZONE" or schema.get("requests") != "BIGINT":
                    raise _unavailable("invalid empty-hour count reference", label)
                by_path[path] = {}
            else:
                try:
                    by_path[path] = validated_daily_reference_minutes(
                        reader, path, parse_day_key_strict(hour[:10]), allow_empty=not hourly
                    )
                except (RuntimeError, ValueError) as exc:
                    raise _unavailable("invalid retained minute reference", label) from exc
        minutes = by_path[path]
        end = start + timedelta(hours=1)
        hour_minutes = {bucket: count for bucket, count in minutes.items() if start <= bucket < end}
        if hourly and len(hour_minutes) != len(minutes):
            raise _unavailable("invalid retained reference scope", label)
        full_day_hours = len({bucket.hour for bucket in minutes}) == 24
        if hour not in hours:
            for minute in requested_minutes[hour]:
                if hourly or minute in minutes or full_day_hours:
                    count = minutes.get(minute, 0)
                    expected[minute.isoformat()] = (
                        minute,
                        minute + timedelta(minutes=1),
                        {minute: count} if count else {},
                    )
                else:
                    day_start = parse_day_key_strict(hour[:10])
                    expected[hour[:10]] = (
                        day_start,
                        day_start + timedelta(days=1),
                        {bucket: count for bucket, count in minutes.items() if count},
                    )
        elif hourly or bool(hour_minutes) or full_day_hours:
            expected[hour] = (start, end, {bucket: count for bucket, count in hour_minutes.items() if count})
        else:
            # A sparse daily subset cannot prove an absent minute is zero.
            day_start = parse_day_key_strict(hour[:10])
            expected[hour[:10]] = (
                day_start,
                day_start + timedelta(days=1),
                {bucket: count for bucket, count in minutes.items() if count},
            )
    return expected, generations


@contextmanager
def verified_boundary_table(
    reader: Reader,
    base_table: str,
    columns_sql: str,
    ranges: list[Range],
    proof_hours: list[str],
    *,
    label: str = "analytics",
    proof_minutes: list[datetime] | None = None,
    empty_old_hours: list[str] | None = None,
    projection_params: list[Any] | None = None,
    phase: Callable[[str, float], None] | None = None,
) -> Iterator[str | None]:
    """Proof and exact-edge aggregates consume one and the same raw materialization."""
    proofs, references = minute_reference_proofs(reader, proof_hours, label, proof_minutes)
    extra = [
        (parse_hour_key_strict(hour), parse_hour_key_strict(hour) + timedelta(hours=1), False)
        for hour in empty_old_hours or []
    ]
    scan_ranges = merge_ranges([*ranges, *((start, end, False) for start, end, _ in proofs.values()), *extra])
    if not scan_ranges:
        yield None
        return
    if proofs and phase:
        phase(
            "verified_old_boundary:reference_hours",
            sum(
                (end - start).total_seconds() / 3600
                for start, end, _ in merge_ranges([(start, end, False) for start, end, _ in proofs.values()])
            ),
        )
    branches = [
        f"SELECT {columns_sql} FROM (SELECT * FROM {base_table} WHERE {range_sql([part])})" for part in scan_ranges
    ]
    table = f"t_exact_boundary_{uuid.uuid4().hex}"
    reader.execute(
        f"CREATE TEMP TABLE {table} AS {' UNION ALL '.join(branches)}",
        (projection_params or []) * len(branches),
    )
    try:
        if proofs:
            raw_minutes = dict(
                reader.execute(
                    f"SELECT time_bucket(INTERVAL '1 minute', timestamp::TIMESTAMPTZ), count(*) "
                    f'FROM "{table}" GROUP BY 1'
                ).fetchall()
            )
            for start, end, expected in proofs.values():
                actual = {bucket: count for bucket, count in raw_minutes.items() if start <= bucket < end}
                if actual != expected:
                    raise _unavailable("retained raw/minute reference mismatch", label)
            for path, saved in references.items():
                if generation(path) != saved:
                    raise _unavailable("retained minute reference changed", label)
        if (
            extra
            and reader.execute(f'SELECT count(*) FROM "{table}" WHERE {range_sql(merge_ranges(extra))}').fetchone()[0]
        ):
            raise _unavailable("old raw has no canonical count reference", label)
        yield table
    finally:
        _drop_temp_table(reader, table, "verified_boundary_table")


def has_retained_day(src: dict[str, Any], day: str) -> bool:
    root = _day_bundled_root(src)
    if any(
        _day_bundle_file(root, day, name)
        for name in (DAY_BUNDLE_FILENAME, TIME_SERIES_BUNDLE_FILENAME, OVERVIEW_BUNDLE_FILENAME)
    ):
        return True
    fields = _day_rollups_root(src)
    return any(
        file.endswith(".parquet") and not file.startswith(".tmp_")
        for field in cached_listdir(fields)
        if field.startswith("field=")
        for file in cached_listdir(os.path.join(fields, field, f"day={day}"))
    )


def retained_window_requires_exact(
    src: dict[str, Any], start_time: str | None, end_time: str | None, selected: list[str] | None
) -> bool:
    """Do not heal/fallback over daily history the legacy prefix plan omitted."""
    if not start_time or not end_time:
        return False
    st, et = parse_iso_utc(start_time), parse_iso_utc(end_time)
    if st is None or et is None or et <= st or et - st > timedelta(days=366):
        return False
    day = st.replace(hour=0, minute=0, second=0, microsecond=0)
    root = _day_bundled_root(src)
    while day < et:
        key = day.strftime("%Y-%m-%d")
        ts = _day_bundle_file(root, key, TIME_SERIES_BUNDLE_FILENAME)
        if (ts and ts not in (selected or [])) or (selected is None and has_retained_day(src, key)):
            return True
        day += timedelta(days=1)
    return False


@dataclass(frozen=True)
class ExactMinutePlan:
    start: datetime
    end: datetime
    paths: list[str]
    overview_paths: list[str]
    raw_ranges: list[Range]
    proof_minutes: list[datetime]
    empty_old_hours: list[str]
    recent_missing: int


def plan_exact_minutes(
    src: dict[str, Any], st: datetime, et: datetime, *, overview: bool, inclusive: bool
) -> ExactMinutePlan:
    """Select all retained minute/daily interiors, not just a contiguous prefix."""
    start = st.replace(second=0, microsecond=0)
    if start < st:
        start += timedelta(minutes=1)
    end = et.replace(second=0, microsecond=0)
    index = CoverageIndex.build(st, et)
    cutoff = index.active_dt - timedelta(hours=MISSING_HOUR_HEAL_CAP)
    probe = BundleProbe(_hour_bundled_root(src))
    day_root = _day_bundled_root(src)
    paths: set[str] = set()
    overview_paths: set[str] = set()
    ranges: list[Range] = []
    empty_old: set[str] = set()
    recent_missing = 0
    day_refs: dict[str, str | None] = {}
    known_days: dict[str, bool] = {}

    def known(day: str) -> bool:
        if day not in known_days:
            known_days[day] = has_retained_day(src, day)
        return known_days[day]

    for day, hours in index.by_day().items():
        day_refs[day] = _day_bundle_file(day_root, day, TIME_SERIES_BUNDLE_FILENAME)
        day_start = parse_day_key_strict(day)
        lo, hi = max(start, day_start), min(end, day_start + timedelta(days=1))
        ov = (
            _day_bundle_file(day_root, day, OVERVIEW_BUNDLE_FILENAME)
            if overview
            and lo.minute == lo.second == lo.microsecond == 0
            and hi.minute == hi.second == hi.microsecond == 0
            else None
        )
        if day_refs[day]:
            paths.add(str(day_refs[day]))
            continue
        if ov:
            overview_paths.add(ov)
            continue
        for hour in hours:
            h_start = parse_hour_key_strict(hour)
            if probe.has(hour, TIME_SERIES_BUNDLE_FILENAME):
                paths.add(probe.path(hour, TIME_SERIES_BUNDLE_FILENAME))
            elif probe.is_empty(hour, DAY_BUNDLE_FILENAME):
                continue
            elif h_start >= cutoff:
                lo, hi = max(start, h_start), min(end, h_start + timedelta(hours=1))
                if lo < hi:
                    ranges.append((lo, hi, False))
                    recent_missing += 1
            elif probe.has(hour, DAY_BUNDLE_FILENAME) or known(day):
                raise _unavailable("retained day has no readable minute reference")
            else:
                # Missing artifacts do not prove retirement OR empty prehistory.
                # Check unknown old gaps in the same bounded materialization;
                # nonempty unreferenced raw cannot silently become zero history.
                empty_old.add(hour)
    if index.crosses_active:
        lo, hi = max(start, index.active_dt), end
        if lo < hi:
            ranges.append((lo, hi, False))
    if st < min(start, et):
        ranges.append((st, min(start, et), False))
    if max(st, end) < et:
        ranges.append((max(st, end), et, False))
    if inclusive:
        ranges.append((et, et, True))
    ranges = merge_ranges(ranges)
    proofs: set[datetime] = set()
    for lo, hi, inclusive_range in ranges:
        for bound in (lo, hi if inclusive_range else hi - timedelta(microseconds=1)):
            hour_start = bound.replace(minute=0, second=0, microsecond=0)
            if hour_start >= cutoff:
                continue
            hour = hour_start.strftime("%Y-%m-%d-%H")
            if reference_exists(src, hour) or known(hour[:10]):
                proofs.add(bound.replace(second=0, microsecond=0))
            else:
                empty_old.add(hour)
    return ExactMinutePlan(
        start, end, sorted(paths), sorted(overview_paths), ranges, sorted(proofs), sorted(empty_old), recent_missing
    )
