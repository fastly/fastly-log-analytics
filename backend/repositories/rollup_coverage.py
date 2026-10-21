"""Closed-hour coverage index and rollup read plans.

Every ``try_*_from_rollup`` reader decides, from the same on-disk facts,
whether a window can be served from rollup parquets or must fall back to live
SQL: which UTC hours are closed, which of them have a bundle (or a
verified-empty sentinel), whether the writer is behind, and whether the window
reaches into the active hour. That decision used to be re-implemented inline
in each reader; it lives here once.

* :class:`CoverageIndex` is the immutable closed-hour grid for one window.
  The active-hour boundary is captured a single time, so every check made for
  one request agrees about which hours are closed.
* :class:`BundleProbe` memoizes the filesystem probes made against one
  hour-bundled root for the duration of ONE read. It is never shared between
  requests and never outlives the call that created it: there is no result
  cache here, only the 60 s directory-listing memo inherited from the old
  walk.
* :func:`plan_hourly_read` is the fail-closed plan used by the minute-grain
  time-series readers; :func:`collect_day_prefer_paths` and
  :func:`collect_hour_paths_with_floor` are the tolerant leaderboard plans.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from backend import config as svcconfig
from backend.core.rollup_readiness import rollup_coverage_ready
from backend.core.rollups._common import (
    DAY_BUNDLE_FILENAME,
    _day_bundled_root,
    _hour_bundled_root,
    _rollups_root,
)
from backend.utils.date_utils import parse_hour_key_strict, parse_window

ALL_FIELDS_FILENAME = "all_fields.parquet"

# Closed-hour coverage below this fraction makes a tolerant (top-N / percentile)
# reader fall back to live SQL instead of serving a misleading sample.
MIN_CLOSED_HOUR_COVERAGE = 0.5

# Missing-hour live heal bound: only this many of the most recent closed hours
# are live-queried when the writer is behind. Older gaps make the reader fall
# back to a raw scan instead.
MISSING_HOUR_HEAL_CAP = 48

# Cache for ``os.listdir`` on the rollup directory tree. The coverage walk
# lists every field dir once per request; on prod that is ~80 listdirs of ~375
# entries and 1.3-3 s of pure stat work (perf audit F5). The cron sync
# rebuilds the tree at most every minute, so a 60 s TTL never serves rollup
# output more than one tick stale. Bounded by entry count so unbounded
# service/hour churn cannot blow it up.
LISTDIR_CACHE_TTL_S = 60.0
LISTDIR_CACHE_MAX_ENTRIES = 32768
_listdir_cache: dict[str, tuple[float, list[str]]] = {}


def cached_listdir(path: str) -> list[str]:
    """Return ``os.listdir(path)`` cached for ``LISTDIR_CACHE_TTL_S``.

    Returns ``[]`` on any OSError (callers treat missing and empty
    directories the same). The cache is intentionally simple: no per-entry
    expiry sweep, just a flat-clear when full.
    """
    now = time.monotonic()
    cached = _listdir_cache.get(path)
    if cached is not None and (now - cached[0]) < LISTDIR_CACHE_TTL_S:
        return cached[1]
    try:
        entries = os.listdir(path)
    except OSError:
        entries = []
    if len(_listdir_cache) >= LISTDIR_CACHE_MAX_ENTRIES:
        _listdir_cache.clear()
    _listdir_cache[path] = (now, entries)
    return entries


def clear_listdir_cache() -> None:
    """Drop the cached rollup listdir entries. Used by tests + the sync
    writer's commit hook when fresh files have been written."""
    _listdir_cache.clear()


def rc_service_id(src: dict) -> str:
    """Service key used by the durable-serving readiness flag.

    ``service_id`` is always present on the source dict
    ``get_source_for_service`` returns; ``name`` is the display fallback for
    older/local configs.
    """
    return src.get("service_id") or src.get("name") or ""


def durable_serving_blocked(src: dict) -> bool:
    """True when a durable serving replica must not trust its local rollup tree.

    Rollups are pod-local accelerators: a missing local tree is not evidence
    that the durable DuckLake table is empty or partially covered, so until
    the pod confirms coverage every rollup reader falls back to durable SQL.
    """
    return svcconfig.is_durable_serving_mode(src) and not rollup_coverage_ready(rc_service_id(src))


def eligible_window(
    start_time: str | None,
    end_time: str | None,
    *,
    has_filters: bool,
    require_top_asns: list[int] | None = None,
    min_hours: int,
) -> tuple[datetime, datetime] | None:
    """Parse and gate a request window shared by the rollup readers.

    Returns the UTC ``(st, et)`` when every guard passes, else ``None``.
    Guards, in order: unfiltered only; both bounds present; optional non-empty
    ``require_top_asns``; both bounds parse with ``et > st``; the window spans
    at least ``min_hours`` and at most 366 days. Every guard is a
    side-effect-free ``return None``.
    """
    if has_filters:
        return None
    if not start_time or not end_time:
        return None
    if require_top_asns is not None and not require_top_asns:
        return None
    window = parse_window(start_time, end_time)
    if window is None:
        return None
    st, et = window
    if (et - st) < timedelta(hours=min_hours):
        return None
    if (et - st) > timedelta(days=366):
        return None
    return st, et


def meets_coverage_floor(covered_hours: int, total_closed_hours: int) -> bool:
    """Whether ``covered_hours`` of ``total_closed_hours`` clears the tolerant floor."""
    return not (total_closed_hours > 0 and (covered_hours / total_closed_hours) < MIN_CLOSED_HOUR_COVERAGE)


@dataclass(frozen=True)
class CoverageIndex:
    """Closed-hour grid for one ``[st, et)`` window.

    ``closed_hours`` are the ``YYYY-MM-DD-HH`` keys of every UTC hour in the
    window strictly before the active hour, oldest first. ``crosses_active``
    is set when the window extends into (or past) the active hour.
    """

    st: datetime
    et: datetime
    active_dt: datetime
    active_hour_str: str
    closed_hours: tuple[str, ...]
    crosses_active: bool

    @classmethod
    def build(
        cls,
        st: datetime,
        et: datetime,
        *,
        skip_partial_start: bool = False,
        now: datetime | None = None,
    ) -> CoverageIndex:
        """Walk ``[st, et)`` hour by hour.

        ``skip_partial_start`` drops a mid-hour ``st``'s leading partial hour
        (rollups are whole-hour grains, so serving it would over-count);
        without it the walk starts at the hour containing ``st``.
        """
        now = now if now is not None else datetime.now(UTC)
        active_hour_str = now.strftime("%Y-%m-%d-%H")
        active_dt = parse_hour_key_strict(active_hour_str)
        cursor = st.replace(minute=0, second=0, microsecond=0)
        if skip_partial_start and cursor < st:
            cursor += timedelta(hours=1)
        hours: list[str] = []
        crosses_active = False
        while cursor < et:
            key = cursor.strftime("%Y-%m-%d-%H")
            if key >= active_hour_str:
                crosses_active = True
                break
            hours.append(key)
            cursor += timedelta(hours=1)
        return cls(st, et, active_dt, active_hour_str, tuple(hours), crosses_active)

    def by_day(self) -> dict[str, list[str]]:
        """Closed hours bucketed by their ``YYYY-MM-DD`` day, oldest day first."""
        days: dict[str, list[str]] = {}
        for hour in self.closed_hours:
            days.setdefault(hour[:10], []).append(hour)
        return days


def _empty_parquet(path: str) -> bool:
    try:
        import pyarrow.parquet as pq

        return pq.read_metadata(path).num_rows == 0
    except Exception:
        return False


def is_empty_bundle(path: str) -> bool:
    """Return whether a parquet bundle is the verified-empty sentinel."""
    if not os.path.isfile(path):
        return False
    return _empty_parquet(path)


class BundleProbe:
    """Memoized probes against one hour-bundled root, scoped to a single read."""

    __slots__ = ("_empty", "_has", "root")

    def __init__(self, bundled_root: str) -> None:
        self.root = bundled_root
        self._has: dict[tuple[str, str], bool] = {}
        self._empty: dict[tuple[str, str], bool] = {}

    def path(self, hour: str, filename: str) -> str:
        return os.path.join(self.root, f"hour={hour}", filename)

    def has(self, hour: str, filename: str) -> bool:
        key = (hour, filename)
        found = self._has.get(key)
        if found is None:
            found = self._has[key] = os.path.isfile(self.path(hour, filename))
        return found

    def is_empty(self, hour: str, filename: str) -> bool:
        key = (hour, filename)
        empty = self._empty.get(key)
        if empty is None:
            empty = self._empty[key] = self.has(hour, filename) and _empty_parquet(self.path(hour, filename))
        return empty


def _per_field_hours(src: dict) -> set[str]:
    """Union of every ``hour=…`` entry across all per-field rollup dirs.

    One pass over the (60 s cached) listings turns each "did this hour have
    data?" question into an O(1) lookup. The previous shape probed
    ``os.path.isdir`` per (hour, field) — ~11.8k syscalls for a 7-day window
    over ~70 fields.
    """
    root = _rollups_root(src)
    hours: set[str] = set()
    for field_dir in cached_listdir(root):
        if not field_dir.startswith("field="):
            continue
        for entry in cached_listdir(os.path.join(root, field_dir)):
            if entry.startswith("hour="):
                hours.add(entry[len("hour=") :])
    return hours


def _walk_hourly_bundles(
    src: dict,
    index: CoverageIndex,
    probe: BundleProbe,
    bundle_filename: str,
    allow_all_fields_coverage: bool,
) -> list[str] | None:
    paths: list[str] = []
    per_field_hours: set[str] | None = None
    for hour in index.closed_hours:
        if probe.has(hour, bundle_filename):
            paths.append(probe.path(hour, bundle_filename))
            continue
        if allow_all_fields_coverage and probe.has(hour, ALL_FIELDS_FILENAME):
            continue
        if per_field_hours is None:
            per_field_hours = _per_field_hours(src)
        if hour in per_field_hours:
            # The writer is behind: the hour has per-field rollup data but no
            # bundle yet, so serving the rollup would undercount.
            return None
    return paths


def collect_hourly_bundle_paths(
    src: dict,
    st: datetime,
    et: datetime,
    bundled_root: str,
    bundle_filename: str,
    *,
    allow_all_fields_coverage: bool = False,
) -> tuple[list[str], bool] | None:
    """Walk ``[st, et)`` by UTC hour, return ``(paths, crosses_active)``.

    ``paths`` are the per-hour bundles that exist on disk and
    ``crosses_active`` is set when the window extends into (or past) the live
    hour. Returns ``None`` if any closed hour has per-field rollup data but no
    bundle on disk — the writer-behind case where serving the rollup would
    undercount, so the caller falls back to raw.

    ``allow_all_fields_coverage`` is limited to the scalar time-series
    readers, where an ``all_fields.parquet`` file proves the hour-bundling
    pass reached the hour even when the time-series bundle is absent.
    """
    if durable_serving_blocked(src):
        return None
    index = CoverageIndex.build(st, et)
    paths = _walk_hourly_bundles(src, index, BundleProbe(bundled_root), bundle_filename, allow_all_fields_coverage)
    if paths is None:
        return None
    return paths, index.crosses_active


def find_missing_bundle_hours(
    st: datetime,
    et: datetime,
    bundled_root: str,
    bundle_filename: str,
    cap: int = MISSING_HOUR_HEAL_CAP,
    coverage_bundle_filename: str | None = None,
) -> list[str]:
    """Closed hours in ``[st, et)`` with no ``bundle_filename`` — a
    writer-coverage gap the caller should live-heal, not treat as zero.

    When ``coverage_bundle_filename`` is given, an hour whose coverage file is
    the verified-empty sentinel counts as covered. Bounded by ``cap``: the
    walk starts no earlier than ``cap`` hours before the active hour, so a
    near-366-day window never stats more than ``cap`` hours.
    """
    now = datetime.now(UTC)
    active_dt = now.replace(minute=0, second=0, microsecond=0)
    floor = max(st, active_dt - timedelta(hours=cap))
    index = CoverageIndex.build(floor, et, now=now)
    return _missing_hours(index, BundleProbe(bundled_root), bundle_filename, coverage_bundle_filename)


def _missing_hours(
    index: CoverageIndex,
    probe: BundleProbe,
    bundle_filename: str,
    coverage_bundle_filename: str | None,
) -> list[str]:
    return [
        hour
        for hour in index.closed_hours
        if not probe.has(hour, bundle_filename)
        and (coverage_bundle_filename is None or not probe.is_empty(hour, coverage_bundle_filename))
    ]


def heal_where_clause(missing_hours: list[str], st: datetime | None, et: datetime | None) -> str:
    """WHERE clause that live-queries exactly ``missing_hours`` from the raw base table.

    The timestamp range is clamped to the request window so a mid-hour window
    edge cannot over-count rows outside it; the ``IN`` list prunes to the
    un-rolled hours in between.
    """
    start = parse_hour_key_strict(missing_hours[0])
    if st and st > start:
        start = st
    end = parse_hour_key_strict(missing_hours[-1]) + timedelta(hours=1)
    if et and et < end:
        end = et
    hours_in_sql = ", ".join(f"'{h}'" for h in missing_hours)
    return (
        f"timestamp >= '{start.isoformat()}' "
        f"AND timestamp < '{end.isoformat()}' "
        f"AND strftime(timestamp, '%Y-%m-%d-%H') IN ({hours_in_sql})"
    )


@dataclass(frozen=True)
class HourlyReadPlan:
    """Fail-closed plan for the minute-grain time-series readers."""

    index: CoverageIndex
    paths: list[str]
    missing_hours: list[str]

    @property
    def crosses_active(self) -> bool:
        return self.index.crosses_active

    @property
    def active_hour_str(self) -> str:
        return self.index.active_hour_str

    @property
    def active_dt(self) -> datetime:
        return self.index.active_dt

    def heal_where(self) -> str:
        """WHERE clause that live-heals ``missing_hours`` from the raw base table."""
        return heal_where_clause(self.missing_hours, self.index.st, self.index.et)


def plan_hourly_read(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
    *,
    allow_all_fields_coverage: bool = False,
    heal: bool = False,
) -> HourlyReadPlan | None:
    """Plan a rollup read of ``bundle_filename`` over ``[st, et)``, or ``None``.

    ``None`` means "fall back to the raw scan": no hour-bundled root, a
    durable replica that has not confirmed coverage, a closed hour whose
    bundle is still being built, or nothing to read at all.

    With ``heal`` the plan also lists closed hours that have neither a bundle
    nor a verified-empty ``all_fields.parquet`` sentinel (``missing_hours``,
    capped at ``MISSING_HOUR_HEAL_CAP``) for the caller to live-query. A
    window whose gaps are not all within that cap is refused rather than
    served truncated.
    """
    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return None
    if durable_serving_blocked(src):
        return None

    index = CoverageIndex.build(st, et)
    probe = BundleProbe(bundled_root)
    paths = _walk_hourly_bundles(src, index, probe, bundle_filename, allow_all_fields_coverage)
    if paths is None:
        return None
    if not paths and not index.crosses_active:
        return None
    if not heal:
        return HourlyReadPlan(index, paths, [])

    # An hour is covered by its bundle or by a verified-empty all_fields
    # sentinel; everything else is a gap. Only gaps within the heal cap can be
    # live-queried, so a window with an older gap is refused rather than
    # served truncated.
    gaps = _missing_hours(index, probe, bundle_filename, ALL_FIELDS_FILENAME)
    heal_floor = max(
        index.st.replace(minute=0, second=0, microsecond=0), index.active_dt - timedelta(hours=MISSING_HOUR_HEAL_CAP)
    )
    heal_floor_key = heal_floor.strftime("%Y-%m-%d-%H")
    missing = [hour for hour in gaps if hour >= heal_floor_key]
    if len(missing) < len(gaps):
        return None
    return HourlyReadPlan(index, paths, missing)


def _day_bundle_file(day_root: str, day: str, bundle_filename: str) -> str | None:
    day_file = os.path.join(day_root, f"day={day}", bundle_filename)
    if os.path.isfile(day_file):
        return day_file
    if bundle_filename == DAY_BUNDLE_FILENAME:
        alt_day_file = os.path.join(day_root, f"day={day}", f"day_bundle_{day}.parquet")
        flat_day_file = os.path.join(day_root, f"day_bundle_{day}.parquet")
        if os.path.isfile(alt_day_file):
            return alt_day_file
        if os.path.isfile(flat_day_file):
            return flat_day_file
    return None


def collect_day_prefer_paths(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
    *,
    skip_partial_start: bool = True,
) -> list[str] | None:
    """Day-prefer / hour-fallback rollup paths for ``[st, et)``, or ``None``.

    For each fully covered UTC day (24 in-window closed hours) the per-day
    compacted file is preferred, else the per-hour bundles are used. ``None``
    on any eligibility miss — no bundled root, no paths, or less than
    ``MIN_CLOSED_HOUR_COVERAGE`` of the closed hours covered.
    """
    if durable_serving_blocked(src):
        return None

    hour_root = _hour_bundled_root(src)
    day_root = _day_bundled_root(src)
    if not os.path.isdir(hour_root) and not os.path.isdir(day_root):
        return None

    index = CoverageIndex.build(st, et, skip_partial_start=skip_partial_start)
    paths: list[str] = []
    covered_hours = 0
    for day, hours_in_day in sorted(index.by_day().items()):
        chosen_day = _day_bundle_file(day_root, day, bundle_filename)
        # The per-day file is only valid when the window spans the ENTIRE UTC
        # day; a partial day would pick up out-of-range hours.
        if len(hours_in_day) == 24 and chosen_day:
            paths.append(chosen_day)
            covered_hours += 24
        else:
            for hour in hours_in_day:
                path = os.path.join(hour_root, f"hour={hour}", bundle_filename)
                if os.path.isfile(path):
                    paths.append(path)
                    covered_hours += 1

    if not paths:
        return None
    if not meets_coverage_floor(covered_hours, len(index.closed_hours)):
        return None
    return paths


def collect_hour_paths_with_floor(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
) -> list[str] | None:
    """Per-hour-only paths for ``[st, et)`` with the tolerant coverage floor.

    Used by the slow-URL panel, which has no day compaction: gaps are
    tolerated down to ``MIN_CLOSED_HOUR_COVERAGE`` because a missing hour or
    two does not change which URLs dominate a multi-day ranking. ``None`` on
    no files or insufficient coverage.
    """
    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return None
    index = CoverageIndex.build(st, et, skip_partial_start=True)
    paths = [
        path
        for path in (os.path.join(bundled_root, f"hour={hour}", bundle_filename) for hour in index.closed_hours)
        if os.path.exists(path)
    ]
    if not paths:
        return None
    if not meets_coverage_floor(len(paths), len(index.closed_hours)):
        return None
    return paths


# R-1: register the listdir memo so the autouse fixture in tests/conftest.py
# drains it via CacheRegistry.clear_all() — it would otherwise leak between
# tests that reuse the same tmp paths.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("repositories.rollup_coverage._listdir_cache", _listdir_cache)
