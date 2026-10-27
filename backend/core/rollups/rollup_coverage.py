"""Rollup partition discovery, coverage indexing, and window read planning."""

from __future__ import annotations

import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow.parquet as pq
from cachetools import TTLCache

from backend import config as svcconfig
from backend.models.analytics import HourlyRollupCoverage, ReaderCoverage, RollupPathCoverage, ScalarRollupCoverage
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_hour_key_strict, parse_window

from ._common import (
    DAY_BUNDLE_FILENAME,
    OVERVIEW_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    _day_bundled_root,
    _hour_bundled_root,
    _is_safe_ident,
    _rollups_root,
    rollup_coverage_ready,
)

ALL_FIELDS_FILENAME = "all_fields.parquet"
MIN_CLOSED_HOUR_COVERAGE = 0.5
MISSING_HOUR_HEAL_CAP = 48

LISTDIR_CACHE_TTL_S = 60.0
LISTDIR_CACHE_MAX_ENTRIES = 32768
_listdir_cache_lock = threading.Lock()
_listdir_cache: TTLCache[str, list[str]] = TTLCache(
    maxsize=LISTDIR_CACHE_MAX_ENTRIES, ttl=LISTDIR_CACHE_TTL_S, timer=time.monotonic
)
_CacheRegistry.register("repositories.rollup_coverage._listdir_cache", _listdir_cache)


def cached_listdir(path: str) -> list[str]:
    """Return ``os.listdir(path)`` cached for ``LISTDIR_CACHE_TTL_S``."""
    with _listdir_cache_lock:
        cached = _listdir_cache.get(path)
    if cached is not None:
        return list(cached)
    try:
        entries = os.listdir(path)
    except OSError:
        entries = []
    with _listdir_cache_lock:
        if len(_listdir_cache) >= LISTDIR_CACHE_MAX_ENTRIES:
            _listdir_cache.clear()
        _listdir_cache[path] = entries
    return entries


def rc_service_id(src: dict) -> str:
    """Service key used by the durable-serving readiness flag."""
    return src.get("service_id") or src.get("name") or ""


def durable_serving_blocked(src: dict) -> bool:
    """True when a durable serving replica must not trust its local rollup tree."""
    return svcconfig.is_durable_serving_mode(src) and not rollup_coverage_ready(rc_service_id(src))


def eligible_window(
    start_time: str | None,
    end_time: str | None,
    *,
    has_filters: bool,
    require_top_asns: list[int] | None = None,
    min_hours: int,
    observation: ReaderCoverage | None = None,
) -> tuple[datetime, datetime] | None:
    """Parse and gate a request window shared by the rollup readers."""
    if observation is not None:
        observation.minimum_window_hours = min_hours
        observation.eligible = False
    if has_filters:
        if observation is not None:
            observation.eligibility_reason = "filtered"
        return None
    if not start_time or not end_time:
        if observation is not None:
            observation.eligibility_reason = "bounds_missing"
        return None
    if require_top_asns is not None and not require_top_asns:
        if observation is not None:
            observation.eligibility_reason = "top_asns_empty"
        return None
    window = parse_window(start_time, end_time)
    if window is None:
        if observation is not None:
            observation.eligibility_reason = "invalid_window"
        return None
    st, et = window
    if observation is not None:
        observation.window_start, observation.window_end = st.isoformat(), et.isoformat()
    if (et - st) < timedelta(hours=min_hours):
        if observation is not None:
            observation.eligibility_reason = "window_too_short"
        return None
    if (et - st) > timedelta(days=366):
        if observation is not None:
            observation.eligibility_reason = "window_too_long"
        return None
    if observation is not None:
        observation.eligible = True
        observation.eligibility_reason = "eligible"
    return st, et


def meets_coverage_floor(covered_hours: int, total_closed_hours: int) -> bool:
    """Whether ``covered_hours`` of ``total_closed_hours`` clears the tolerant floor."""
    return not (total_closed_hours > 0 and (covered_hours / total_closed_hours) < MIN_CLOSED_HOUR_COVERAGE)


@dataclass(frozen=True)
class CoverageIndex:
    """Closed-hour grid for one ``[st, et)`` window."""

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
        return bool(pq.read_metadata(path).num_rows == 0)
    except Exception:
        return False


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
    """Union of every ``hour=…`` entry across all per-field rollup dirs."""
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
    """Walk ``[st, et)`` by UTC hour, return ``(paths, crosses_active)``."""
    if durable_serving_blocked(src):
        return None
    index = CoverageIndex.build(st, et)
    paths = _walk_hourly_bundles(src, index, BundleProbe(bundled_root), bundle_filename, allow_all_fields_coverage)
    return (paths, index.crosses_active) if paths is not None else None


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
    """WHERE clause that live-queries exactly ``missing_hours`` from the raw base table."""
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
        return heal_where_clause(self.missing_hours, self.index.st, self.index.et)


def plan_hourly_read(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
    *,
    allow_all_fields_coverage: bool = False,
    heal: bool = False,
    observation: HourlyRollupCoverage | None = None,
) -> HourlyReadPlan | None:
    """Plan a rollup read of ``bundle_filename`` over ``[st, et)``, or ``None``."""
    if observation is not None:
        observation.window_start, observation.window_end = st.isoformat(), et.isoformat()
        observation.heal_horizon_hours = MISSING_HOUR_HEAL_CAP if heal else None
    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        if observation is not None:
            observation.reason = "no_bundled_root"
        return None
    if durable_serving_blocked(src):
        if observation is not None:
            observation.reason = "durable_unready"
        return None

    index = CoverageIndex.build(st, et)
    if observation is not None:
        observation.closed_hours = len(index.closed_hours)
        observation.active_hour_start = index.active_dt.isoformat()
    probe = BundleProbe(bundled_root)
    paths = _walk_hourly_bundles(src, index, probe, bundle_filename, allow_all_fields_coverage)
    if paths is None:
        if observation is not None:
            observation.reason = "writer_behind"
        return None
    if observation is not None:
        observation.bundle_hours = len(paths)
    if not paths and not index.crosses_active:
        if observation is not None:
            observation.reason = "no_paths"
        return None
    if not heal:
        if observation is not None:
            observation.reason = "planned"
        return HourlyReadPlan(index, paths, [])

    gaps = _missing_hours(index, probe, bundle_filename, ALL_FIELDS_FILENAME)
    if observation is not None:
        observation.missing_hours = len(gaps)
        observation.verified_empty_hours = len(index.closed_hours) - len(paths) - len(gaps)
        observation.covered_hours = len(index.closed_hours) - len(gaps)
        observation.coverage_fraction = (
            observation.covered_hours / len(index.closed_hours) if index.closed_hours else None
        )
    heal_floor = max(
        index.st.replace(minute=0, second=0, microsecond=0), index.active_dt - timedelta(hours=MISSING_HOUR_HEAL_CAP)
    )
    heal_floor_key = heal_floor.strftime("%Y-%m-%d-%H")
    missing = [hour for hour in gaps if hour >= heal_floor_key]
    if len(missing) < len(gaps):
        if observation is not None:
            observation.reason = "old_gaps"
        return None
    if observation is not None:
        observation.reason = "planned"
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
    allow_empty_with_done: bool = False,
    observation: RollupPathCoverage | None = None,
) -> list[str] | None:
    """Day-prefer / hour-fallback rollup paths for ``[st, et)``, or ``None``."""
    if observation is not None:
        observation.minimum_coverage_fraction = MIN_CLOSED_HOUR_COVERAGE
    if durable_serving_blocked(src):
        if observation is not None:
            observation.reason = "durable_unready"
        return None

    hour_root = _hour_bundled_root(src)
    day_root = _day_bundled_root(src)
    if not os.path.isdir(hour_root) and not os.path.isdir(day_root):
        if observation is not None:
            observation.reason = "no_bundled_root"
        return None

    index = CoverageIndex.build(st, et, skip_partial_start=skip_partial_start)
    paths: list[str] = []
    covered_hours = 0
    day_files = hour_files = done_markers = 0
    for day, hours_in_day in sorted(index.by_day().items()):
        chosen_day = _day_bundle_file(day_root, day, bundle_filename)
        if len(hours_in_day) == 24 and chosen_day:
            paths.append(chosen_day)
            covered_hours += 24
            day_files += 1
        else:
            for hour in hours_in_day:
                path = os.path.join(hour_root, f"hour={hour}", bundle_filename)
                if os.path.isfile(path):
                    paths.append(path)
                    covered_hours += 1
                    hour_files += 1
                elif allow_empty_with_done and os.path.isfile(f"{path}.done"):
                    covered_hours += 1
                    done_markers += 1

    if observation is not None:
        observation.active_hour_start = index.active_dt.isoformat()
        observation.closed_hours = len(index.closed_hours)
        observation.covered_hours = covered_hours
        observation.coverage_fraction = covered_hours / len(index.closed_hours) if index.closed_hours else None
        observation.partial_start_skipped = skip_partial_start and st > st.replace(minute=0, second=0, microsecond=0)
        observation.day_files, observation.hour_files, observation.done_markers = day_files, hour_files, done_markers
    if not paths and not (allow_empty_with_done and covered_hours > 0):
        if observation is not None:
            observation.reason = "no_paths"
        return None
    if not meets_coverage_floor(covered_hours, len(index.closed_hours)):
        if observation is not None:
            observation.reason = "below_coverage_floor"
        return None
    if observation is not None:
        observation.reason = "planned"
    return paths


def collect_hour_paths_with_floor(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
) -> list[str] | None:
    """Per-hour-only paths for ``[st, et)`` with the tolerant coverage floor."""
    if durable_serving_blocked(src):
        return None
    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return None
    index = CoverageIndex.build(st, et, skip_partial_start=True)
    paths = [
        path
        for path in (os.path.join(bundled_root, f"hour={hour}", bundle_filename) for hour in index.closed_hours)
        if os.path.exists(path)
    ]
    if not paths or not meets_coverage_floor(len(paths), len(index.closed_hours)):
        return None
    return paths


@dataclass(frozen=True)
class TimeSeriesRollupWindowPlan:
    rollup_paths: list[str]
    day_ov_paths: list[str]
    crosses_active: bool
    active_hour_str: str
    active_hour_dt: datetime
    missing_hours: list[str]
    heal_where_sql: str


@dataclass(frozen=True)
class ClosedHourFieldRollupWindow:
    bundled_paths: list[str]
    per_field_paths: list[str]
    st_dt: datetime
    et_dt: datetime
    active_dt: datetime
    active_end_dt: datetime


def _finalize_scalar_coverage(observation: ScalarRollupCoverage) -> None:
    trailing = observation.trailing
    if (
        trailing is not None
        and trailing.active_hour_start is not None
        and trailing.active_hour_start != observation.active_hour_start
    ):
        observation.reason = "boundary_changed"
        return
    trailing_covered = trailing.covered_hours if trailing is not None else 0
    if trailing_covered is not None:
        observation.covered_hours = observation.prefix_hours + trailing_covered
        observation.coverage_fraction = (
            observation.covered_hours / observation.closed_hours if observation.closed_hours else None
        )


def plan_time_series_rollup_window(
    src: dict[str, Any],
    st: datetime,
    et: datetime,
    *,
    can_use_overview: bool = True,
    coverage_mod: Any = None,
    plan_hourly_read_fn: Any = None,
    observation: ScalarRollupCoverage | None = None,
) -> TimeSeriesRollupWindowPlan | None:
    """Plan closed-day prefix + trailing hourly bundles for time_series/count rollup reads."""
    rollup_coverage = coverage_mod if coverage_mod is not None else sys.modules[__name__]
    plan_hourly_read_impl = plan_hourly_read_fn if plan_hourly_read_fn is not None else plan_hourly_read

    day_ts_paths: list[str] = []
    day_ov_paths: list[str] = []
    trailing_st = st
    cov = rollup_coverage.CoverageIndex.build(st, et)
    if observation is not None:
        observation.window_start, observation.window_end = st.isoformat(), et.isoformat()
        observation.closed_hours = len(cov.closed_hours)
        observation.active_hour_start = cov.active_dt.isoformat()
    if timedelta(days=6) < (et - st) <= timedelta(days=90) and not rollup_coverage.durable_serving_blocked(src):
        day_root = _day_bundled_root(src)
        covered_until_hour: str | None = None
        for day, day_hours in sorted(cov.by_day().items()):
            if day >= cov.active_hour_str[:10]:
                break
            day_ts_file = rollup_coverage._day_bundle_file(day_root, day, TIME_SERIES_BUNDLE_FILENAME)
            day_ov_file = (
                rollup_coverage._day_bundle_file(day_root, day, OVERVIEW_BUNDLE_FILENAME)
                if can_use_overview
                and (len(day_hours) == 24 or (st.minute == 0 and st.second == 0 and st.microsecond == 0))
                else None
            )
            if day_ts_file:
                day_ts_paths.append(day_ts_file)
                covered_until_hour = day_hours[-1]
            elif day_ov_file:
                day_ov_paths.append(day_ov_file)
                covered_until_hour = day_hours[-1]
            else:
                break
            if observation is not None:
                observation.prefix_hours += len(day_hours)
        if covered_until_hour is not None:
            trailing_st = parse_hour_key_strict(covered_until_hour) + timedelta(hours=1)

    if observation is not None:
        observation.trailing_start = trailing_st.isoformat()
        observation.day_time_series_files = len(day_ts_paths)
        observation.day_overview_files = len(day_ov_paths)
    if trailing_st < et:
        if observation is not None:
            observation.trailing = HourlyRollupCoverage()
        plan = plan_hourly_read_impl(
            src,
            trailing_st,
            et,
            TIME_SERIES_BUNDLE_FILENAME,
            allow_all_fields_coverage=True,
            heal=True,
            **({"observation": observation.trailing} if observation is not None else {}),
        )
        if plan is None:
            if observation is not None:
                observation.reason = "trailing_unavailable"
                _finalize_scalar_coverage(observation)
            return None
        rollup_paths = day_ts_paths + list(plan.paths)
        crosses_active = plan.crosses_active
        active_hour_str, active_hour_dt = plan.active_hour_str, plan.active_dt
        missing_hours = list(plan.missing_hours)
        heal_where_sql = plan.heal_where() if plan.missing_hours else ""
    else:
        rollup_paths = day_ts_paths
        crosses_active = False
        active_hour_str, active_hour_dt = cov.active_hour_str, cov.active_dt
        missing_hours = []
        heal_where_sql = ""

    if observation is not None:
        observation.reason = "planned"
        _finalize_scalar_coverage(observation)
    return TimeSeriesRollupWindowPlan(
        rollup_paths=rollup_paths,
        day_ov_paths=day_ov_paths,
        crosses_active=crosses_active,
        active_hour_str=active_hour_str,
        active_hour_dt=active_hour_dt,
        missing_hours=missing_hours,
        heal_where_sql=heal_where_sql,
    )


def enumerate_closed_hour_field_rollups(
    src: dict[str, Any],
    field: str,
    start_time: str | None,
    end_time: str | None,
    *,
    stop_on_first: bool = False,
) -> ClosedHourFieldRollupWindow | None:
    """Enumerate in-window closed-hour bundled (`all_fields.parquet`) and per-field rollup files."""
    from backend.core.duckdb import _cache_dir

    if svcconfig.is_durable_serving_mode(src) or not _is_safe_ident(field) or not start_time or not end_time:
        return None
    try:
        window = parse_window(start_time, end_time)
    except Exception:
        return None
    if window is None:
        return None
    st_dt, et_dt = window

    cache_dir = _cache_dir(src)
    bundled_root = os.path.join(cache_dir, "rollups", "hour_bundled")
    per_field_root = os.path.join(cache_dir, "rollups", "hour", f"field={field}")

    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_end_dt = active_dt + timedelta(hours=1)
    active_str = active_dt.strftime("%Y-%m-%d-%H")
    st_str = st_dt.strftime("%Y-%m-%d-%H")
    et_inclusive = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")

    def _in_window(hour: str) -> bool:
        return st_str <= hour <= et_inclusive and hour < active_str

    bundled_paths: list[str] = []
    bundled_hours: set[str] = set()
    if os.path.isdir(bundled_root):
        try:
            for entry in os.listdir(bundled_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[5:]
                if not _in_window(hour):
                    continue
                p = os.path.join(bundled_root, entry, "all_fields.parquet")
                if os.path.isfile(p):
                    bundled_paths.append(p)
                    bundled_hours.add(hour)
                    if stop_on_first:
                        return ClosedHourFieldRollupWindow(
                            bundled_paths=bundled_paths,
                            per_field_paths=[],
                            st_dt=st_dt,
                            et_dt=et_dt,
                            active_dt=active_dt,
                            active_end_dt=active_end_dt,
                        )
        except OSError:
            pass

    per_field_paths: list[str] = []
    if os.path.isdir(per_field_root):
        try:
            for entry in os.listdir(per_field_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[5:]
                if not _in_window(hour) or hour in bundled_hours:
                    continue
                hour_dir = os.path.join(per_field_root, entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                            per_field_paths.append(os.path.join(hour_dir, fname))
                            if stop_on_first:
                                return ClosedHourFieldRollupWindow(
                                    bundled_paths=bundled_paths,
                                    per_field_paths=per_field_paths,
                                    st_dt=st_dt,
                                    et_dt=et_dt,
                                    active_dt=active_dt,
                                    active_end_dt=active_end_dt,
                                )
                except OSError:
                    continue
        except OSError:
            pass

    return ClosedHourFieldRollupWindow(
        bundled_paths=bundled_paths,
        per_field_paths=per_field_paths,
        st_dt=st_dt,
        et_dt=et_dt,
        active_dt=active_dt,
        active_end_dt=active_end_dt,
    )


__all__ = [
    "ALL_FIELDS_FILENAME",
    "BundleProbe",
    "ClosedHourFieldRollupWindow",
    "CoverageIndex",
    "HourlyReadPlan",
    "LISTDIR_CACHE_MAX_ENTRIES",
    "LISTDIR_CACHE_TTL_S",
    "MIN_CLOSED_HOUR_COVERAGE",
    "MISSING_HOUR_HEAL_CAP",
    "TimeSeriesRollupWindowPlan",
    "_day_bundle_file",
    "_empty_parquet",
    "_finalize_scalar_coverage",
    "_listdir_cache",
    "_listdir_cache_lock",
    "_missing_hours",
    "_per_field_hours",
    "_walk_hourly_bundles",
    "cached_listdir",
    "collect_day_prefer_paths",
    "collect_hour_paths_with_floor",
    "collect_hourly_bundle_paths",
    "durable_serving_blocked",
    "eligible_window",
    "enumerate_closed_hour_field_rollups",
    "heal_where_clause",
    "meets_coverage_floor",
    "plan_hourly_read",
    "plan_time_series_rollup_window",
    "rc_service_id",
]
