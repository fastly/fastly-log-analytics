"""Shared rollup window planners for repository readers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


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


def plan_time_series_rollup_window(
    src: dict[str, Any],
    st: datetime,
    et: datetime,
    *,
    can_use_overview: bool = True,
    coverage_mod: Any = None,
    plan_hourly_read_fn: Any = None,
) -> TimeSeriesRollupWindowPlan | None:
    """Plan closed-day prefix + trailing hourly bundles for time_series/count rollup reads."""
    from backend.core.rollups import TIME_SERIES_BUNDLE_FILENAME
    from backend.core.rollups._common import OVERVIEW_BUNDLE_FILENAME, _day_bundled_root
    from backend.utils.date_utils import parse_hour_key_strict

    from . import rollup_coverage as _default_coverage
    from .rollup_coverage import plan_hourly_read as _default_plan_hourly_read

    rollup_coverage = coverage_mod if coverage_mod is not None else _default_coverage
    plan_hourly_read = plan_hourly_read_fn if plan_hourly_read_fn is not None else _default_plan_hourly_read

    day_ts_paths: list[str] = []
    day_ov_paths: list[str] = []
    trailing_st = st
    cov = rollup_coverage.CoverageIndex.build(st, et)
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
        if covered_until_hour is not None:
            trailing_st = parse_hour_key_strict(covered_until_hour) + timedelta(hours=1)

    if trailing_st < et:
        plan = plan_hourly_read(
            src,
            trailing_st,
            et,
            TIME_SERIES_BUNDLE_FILENAME,
            allow_all_fields_coverage=True,
            heal=True,
        )
        if plan is None:
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
    from backend import config as svcconfig
    from backend.core.duckdb import _cache_dir
    from backend.core.rollups._common import _is_safe_ident
    from backend.utils.date_utils import parse_window

    if svcconfig.is_durable_serving_mode(src):
        return None
    if not _is_safe_ident(field):
        return None
    if not start_time or not end_time:
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


def collapse_hourly_to_daily_bundle_paths(
    src: dict[str, Any],
    rollup_paths: list[str],
    bundle_filename: str,
) -> list[str]:
    """Replace any full 24-hour closed day in ``rollup_paths`` with its compacted
    ``day_bundled/day=YYYY-MM-DD/{bundle_filename}`` file when present on disk."""
    from backend.core.rollups._common import _day_bundled_root

    if len(rollup_paths) < 24:
        return rollup_paths
    day_root = _day_bundled_root(src)
    if not os.path.isdir(day_root):
        return rollup_paths

    by_day: dict[str, list[str]] = {}
    other_paths: list[str] = []
    for p in rollup_paths:
        parent = os.path.basename(os.path.dirname(p))
        if parent.startswith("hour=") and len(parent) >= 18:
            day_key = parent[5:15]
            by_day.setdefault(day_key, []).append(p)
        else:
            other_paths.append(p)

    collapsed: list[str] = list(other_paths)
    for day_key, paths in sorted(by_day.items()):
        if len(paths) == 24:
            day_file = os.path.join(day_root, f"day={day_key}", bundle_filename)
            if os.path.isfile(day_file):
                collapsed.append(day_file)
                continue
        collapsed.extend(paths)
    return collapsed
