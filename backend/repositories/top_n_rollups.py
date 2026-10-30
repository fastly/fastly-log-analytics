"""Top-N and IP-spread rollup path planning, SQL building, and HLL sketch folding."""

from __future__ import annotations

import contextlib
import heapq
import math
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import datetime, timedelta
from typing import Any

from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME, quote_path_list
from backend.core.rollups.rollup_coverage import CoverageIndex, _day_bundle_file, cached_listdir
from backend.core.sql_validator import escape_sql_literal
from backend.models.analytics import TopNRollupCoverage
from backend.utils.date_utils import parse_day_key_strict
from backend.utils.hll import SERIALIZATION_VERSION, HyperLogLog

TopNRow = tuple[str, Any, int]
_EDGE_PREFIXES = ("cmcd_", "edge_", "rum_")


def live_topn_fields(
    safe_fields: Iterable[str], skip_fields: frozenset[str], log_fields: Mapping[str, Any] | None
) -> list[str]:
    """Fields that get a live (non-rolled) top-up, in ``safe_fields`` order."""
    custom_fields = {
        cf["name"]
        for cf in (log_fields or {}).get("custom_fields", [])
        if cf.get("enabled", True) and cf.get("show_in_dashboard", True)
    }
    return [
        f
        for f in safe_fields
        if f not in skip_fields
        and not ((f.startswith(_EDGE_PREFIXES) or f == "fastly_req_id") and f not in custom_fields)
    ]


def fold_rows(*row_sets: Iterable[TopNRow]) -> dict[str, dict[Any, int]]:
    """Sum ``(field, value, count)`` rows from every set into one bucket per field."""
    by_field: dict[str, dict[Any, int]] = {}
    for rows in row_sets:
        for fld, value, count in rows:
            bucket = by_field.setdefault(fld, {})
            bucket[value] = bucket.get(value, 0) + count
    return by_field


def collect_field_totals(rows: list[tuple], totals: dict[str, int]) -> list[TopNRow]:
    """Consume each repeated pre-limit total once; saved tail buckets are not display values."""
    for field, total in {row[0]: row[3] for row in rows}.items():
        totals[field] = totals.get(field, 0) + int(total)
    return [(field, value, count) for field, value, count, _ in rows if value != "__other__"]


def truncate_top_n(
    by_field: Mapping[str, Mapping[Any, int]],
    fields: Iterable[str],
    limit: int,
    per_field_limits: Mapping[str, int] | None,
) -> tuple[list[TopNRow], list[str]]:
    """Top rows per requested field, plus the requested fields with no rows."""
    pfl = per_field_limits or {}
    top_results: list[TopNRow] = []
    empty: list[str] = []
    for field in fields:
        bucket = by_field.get(field)
        if not bucket:
            empty.append(field)
            continue
        field_limit = pfl.get(field, limit)
        items = bucket.items()
        top_items = (
            heapq.nlargest(field_limit, items, key=lambda x: x[1])
            if field_limit < len(bucket)
            else sorted(items, key=lambda x: x[1], reverse=True)
        )
        top_results.extend((field, val, count) for val, count in top_items)
    return top_results, empty


@dataclass
class SchemaSeeds:
    """Lazily resolved column list + column types, shared by the live and heal steps."""

    cols_seed: list[str] | None = None
    types_seed: dict[str, str] | None = None
    cols: list[str] = dc_field(default_factory=list)
    types: dict[str, str] = dc_field(default_factory=dict)

    def ensure(self, get_cols: Callable[[], list[str]], get_types: Callable[[], dict[str, str]]) -> None:
        """Fill ``cols`` then ``types`` if still unset (columns first: a failing         column lookup must not trigger the types lookup)."""
        if not self.cols:
            self.cols = self.cols_seed if self.cols_seed is not None else get_cols()
        if not self.types:
            self.types = self.types_seed if self.types_seed is not None else get_types()


@dataclass
class TopNPaths:
    """Parquet files one top-N read hands to DuckDB, one list per source shape."""

    day_paths: list[str] = dc_field(default_factory=list)
    hour_paths: list[str] = dc_field(default_factory=list)
    bundled_hour_paths: list[str] = dc_field(default_factory=list)
    bundled_day_paths: list[str] = dc_field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.day_paths or self.hour_paths or self.bundled_hour_paths or self.bundled_day_paths)


def qualify_limit_expr(safe_fields: Iterable[str], limit: int, per_field_limits: Mapping[str, int] | None) -> str:
    """SQL expression for the per-field ``QUALIFY`` cutoff."""
    wanted = set(safe_fields)
    pfl = {f: v for f, v in (per_field_limits or {}).items() if f in wanted}
    if not pfl:
        return str(int(limit))
    whens = " ".join(f"WHEN '{f.replace(chr(39), chr(39) * 2)}' THEN {int(v)}" for f, v in pfl.items())
    return f"CASE field {whens} ELSE {int(limit)} END"


def build_rolled_sql(
    paths: TopNPaths,
    safe_fields: list[str],
    limit: int,
    per_field_limits: Mapping[str, int] | None,
    *,
    include_totals: bool = False,
) -> str | None:
    """The rolled-parquet top-N query, or ``None`` when there is nothing to read."""
    if not paths:
        return None
    field_in_list = ", ".join("'" + escape_sql_literal(f) + "'" for f in safe_fields)
    select = "SELECT field, value, CAST(count AS BIGINT) AS count "
    branches: list[str] = [
        f"{select}FROM read_parquet([{quote_path_list(t)}], hive_partitioning=1)"
        for t in (paths.day_paths, paths.hour_paths)
        if t
    ] + [
        f"{select}FROM read_parquet([{quote_path_list(b)}], hive_partitioning=0) WHERE field IN ({field_in_list})"
        for b in (paths.bundled_hour_paths, paths.bundled_day_paths)
        if b
    ]
    total_sql = ", SUM(SUM(count)) OVER (PARTITION BY field) AS field_total" if include_totals else ""
    return (
        f"SELECT field, value, SUM(count) AS c{total_sql} FROM ("
        + " UNION ALL ".join(branches)
        + f") GROUP BY field, value QUALIFY ROW_NUMBER() OVER (PARTITION BY field ORDER BY c DESC) <= {qualify_limit_expr(safe_fields, limit, per_field_limits)}"
    )


@dataclass(frozen=True)
class TopNWindow:
    """One request's window in the hour/day keys the rollup tree is named by."""

    st_dt: datetime | None
    et_dt: datetime | None
    active_dt: datetime
    active_dt_end: datetime
    active_str: str
    st_floor: str | None
    et_floor: str | None

    @classmethod
    def build(cls, st_dt: datetime | None, et_dt: datetime | None, now: datetime) -> TopNWindow:
        active_dt = now.replace(minute=0, second=0, microsecond=0)
        return cls(
            st_dt,
            et_dt,
            active_dt,
            active_dt + timedelta(hours=1),
            active_dt.strftime("%Y-%m-%d-%H"),
            st_dt.strftime("%Y-%m-%d-%H") if st_dt else None,
            (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H") if et_dt else None,
        )

    def day_fully_in_window(self, day_str: str) -> bool:
        """Whether a per-day file's whole 24 h lies inside the request window."""
        try:
            day_start = parse_day_key_strict(day_str)
        except ValueError:
            return False
        if self.st_dt and self.st_dt > day_start:
            return False
        return not (self.et_dt and self.et_dt < day_start + timedelta(days=1))

    def day_in_range(self, day: str) -> bool:
        """Day key is not before the first / after the last window day."""
        if self.st_floor and day < self.st_floor[:10]:
            return False
        return not (self.et_floor and day > self.et_floor[:10])

    def hour_in_range(self, hour: str) -> bool:
        """Closed hour key inside ``[st_floor, et_floor]`` (the active hour is excluded)."""
        if (self.st_floor and hour < self.st_floor) or (self.et_floor and hour > self.et_floor):
            return False
        return hour < self.active_str


@dataclass
class TopNPlan:
    """What the filesystem walk found for one window."""

    paths: TopNPaths
    bundled_hours: set[str]
    bundled_days: set[str]
    days_with_day_files_any: set[str]
    per_field_hours_seen: set[str]
    day_root: str
    bundled_day_root: str

    def uncovered_old_boundary_hours(
        self,
        win: TopNWindow,
        heal_cap: int,
        safe_fields: list[str],
        *,
        has_reference: Callable[[str], bool] | None = None,
    ) -> list[str]:
        """Known old boundary history; references need not have field vectors."""
        if win.st_dt is None or win.et_dt is None:
            return []
        cutoff = win.active_dt - timedelta(hours=heal_cap)
        boundary_days = {win.st_dt.strftime("%Y-%m-%d"), win.et_dt.strftime("%Y-%m-%d")}
        missing: list[str] = []
        for day in boundary_days:
            if win.day_fully_in_window(day):
                continue
            known_day = bool(
                _day_bundle_file(self.bundled_day_root, day, "all_fields.parquet")
                or _day_has_real_rollup(self, safe_fields, day)
            )
            day_start = parse_day_key_strict(day)
            start = max(day_start, win.st_dt)
            hour = start.replace(minute=0, second=0, microsecond=0)
            end = min(day_start + timedelta(days=1), win.et_dt, cutoff)
            while hour < end:
                key = hour.strftime("%Y-%m-%d-%H")
                if (
                    key not in self.bundled_hours
                    and key not in self.per_field_hours_seen
                    and (known_day or (has_reference is not None and has_reference(key)))
                ):
                    # Reference existence establishes history, not raw completeness.
                    # The executor must verify every selected old hour before merging.
                    missing.append(key)
                hour += timedelta(hours=1)
        return sorted(missing)


def _has_real_parquet(directory: str) -> bool:
    return any(f.endswith(".parquet") and not f.startswith(".tmp_") for f in cached_listdir(directory))


def _day_covered_by_any_field(
    day_root: str,
    safe_fields: list[str],
    win: TopNWindow,
    days_with_day_files_any: set[str],
    *,
    fields_with_day_dirs: set[str] | None = None,
) -> set[str]:
    """Closed, fully-in-window days where at least one field has a usable day file."""
    covered: set[str] = set()
    active_day = win.active_str[:10]
    for field in safe_fields:
        field_day_dir = os.path.join(day_root, f"field={field}")
        if not os.path.isdir(field_day_dir):
            continue
        if fields_with_day_dirs is not None:
            fields_with_day_dirs.add(field)
        for day_entry in cached_listdir(field_day_dir):
            if not day_entry.startswith("day="):
                continue
            day = day_entry[len("day=") :]
            if len(day) != 10:
                continue
            days_with_day_files_any.add(day)
            if day in covered or day >= active_day or not win.day_in_range(day) or not win.day_fully_in_window(day):
                continue
            if _has_real_parquet(os.path.join(field_day_dir, day_entry)):
                covered.add(day)
    return covered


def _bundled_day_files(
    bundled_day_root: str, win: TopNWindow, days_with_day_files_any: set[str]
) -> tuple[list[str], set[str]]:
    """Per-day bundles (one parquet holding every field) usable for this window."""
    paths: list[str] = []
    days: set[str] = set()
    if not os.path.isdir(bundled_day_root):
        return paths, days
    active_day = win.active_str[:10]
    for entry in cached_listdir(bundled_day_root):
        b_day, bundle_path = None, None
        if entry.startswith("day="):
            cand_day = entry[len("day=") :]
            if len(cand_day) == 10:
                cand1 = os.path.join(bundled_day_root, entry, "all_fields.parquet")
                cand2 = os.path.join(bundled_day_root, entry, f"day_bundle_{cand_day}.parquet")
                if os.path.isfile(cand1):
                    b_day, bundle_path = cand_day, cand1
                elif os.path.isfile(cand2):
                    b_day, bundle_path = cand_day, cand2
        elif entry.startswith("day_bundle_") and entry.endswith(".parquet"):
            cand_day = entry[len("day_bundle_") : -len(".parquet")]
            if len(cand_day) == 10:
                cand = os.path.join(bundled_day_root, entry)
                if os.path.isfile(cand):
                    b_day, bundle_path = cand_day, cand
        if not b_day or not bundle_path:
            continue
        days_with_day_files_any.add(b_day)
        if b_day >= active_day or not win.day_in_range(b_day) or not win.day_fully_in_window(b_day) or b_day in days:
            continue
        paths.append(bundle_path)
        days.add(b_day)
    return paths, days


def _bundled_hour_files(
    bundled_hour_root: str, win: TopNWindow, bundled_days: set[str], day_covered_by_any_field: set[str]
) -> tuple[list[str], set[str]]:
    """Per-hour bundles for closed in-window hours not already served by a day file."""
    paths: list[str] = []
    hours: set[str] = set()
    if not os.path.isdir(bundled_hour_root):
        return paths, hours
    for hour_entry in cached_listdir(bundled_hour_root):
        if not hour_entry.startswith("hour="):
            continue
        hour = hour_entry[len("hour=") :]
        if not win.hour_in_range(hour) or hour[:10] in bundled_days or hour[:10] in day_covered_by_any_field:
            continue
        bundle_path = os.path.join(bundled_hour_root, hour_entry, "all_fields.parquet")
        if os.path.isfile(bundle_path):
            paths.append(bundle_path)
            hours.add(hour)
    return paths, hours


def _per_field_files(
    safe_fields: list[str],
    day_root: str,
    hour_root: str,
    win: TopNWindow,
    bundled_days: set[str],
    bundled_hours: set[str],
    out: TopNPaths,
    *,
    fields_with_day_dirs: set[str] | None = None,
) -> set[str]:
    """Per-field day files, then per-field hour files for what they don't cover."""
    active_day = win.active_str[:10]
    active_str, st_floor, et_floor = win.active_str, win.st_floor, win.et_floor
    hours_seen: set[str] = set()
    for field in safe_fields:
        field_hour_dir, field_day_dir = (
            os.path.join(hour_root, f"field={field}"),
            os.path.join(day_root, f"field={field}"),
        )
        covered_days: set[str] = set(bundled_days)
        # Reuse the earlier daily walk rather than add a second filesystem probe
        # for every field whose hourly directory has already retired.
        has_day_dir = (
            field in fields_with_day_dirs if fields_with_day_dirs is not None else os.path.isdir(field_day_dir)
        )
        if has_day_dir:
            for day_entry in cached_listdir(field_day_dir):
                if not day_entry.startswith("day="):
                    continue
                day = day_entry[len("day=") :]
                if (
                    len(day) != 10
                    or day in bundled_days
                    or day >= active_day
                    or not win.day_in_range(day)
                    or not win.day_fully_in_window(day)
                ):
                    continue
                day_dir = os.path.join(field_day_dir, day_entry)
                for fname in cached_listdir(day_dir):
                    if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                        out.day_paths.append(os.path.join(day_dir, fname))
                        covered_days.add(day)
        # Daily retention is independent of retired hourly inputs/directories.
        if not os.path.isdir(field_hour_dir):
            continue
        for hour_entry in cached_listdir(field_hour_dir):
            if not hour_entry.startswith("hour="):
                continue
            hour = hour_entry[len("hour=") :]
            if (st_floor and hour < st_floor) or (et_floor and hour > et_floor) or hour >= active_str:
                continue
            if hour[:10] in covered_days or hour in bundled_hours:
                continue
            hour_dir = os.path.join(field_hour_dir, hour_entry)
            for fname in cached_listdir(hour_dir):
                if fname.endswith(".parquet"):
                    out.hour_paths.append(os.path.join(hour_dir, fname))
                    hours_seen.add(hour)
    return hours_seen


def _day_has_real_rollup(plan: TopNPlan, safe_fields: list[str], day: str) -> bool:
    """Whether ``day`` really was day-compacted (a bundle, or a field day dir holding a parquet)."""
    if os.path.isfile(os.path.join(plan.bundled_day_root, f"day={day}", "all_fields.parquet")):
        return True
    return any(
        os.path.isdir(d := os.path.join(plan.day_root, f"field={field}", f"day={day}")) and _has_real_parquet(d)
        for field in safe_fields
    )


def find_unrolled_hours(
    plan: TopNPlan,
    win: TopNWindow,
    safe_fields: list[str],
    heal_cap: int,
    *,
    observation: TopNRollupCoverage | None = None,
) -> list[str]:
    """Closed in-window hours with no rollup coverage, which the caller live-queries."""
    floor = (
        max(win.active_dt - timedelta(hours=heal_cap), win.st_dt)
        if win.st_dt
        else win.active_dt - timedelta(hours=heal_cap)
    )
    candidates = CoverageIndex.build(floor, win.et_dt or win.active_dt, now=win.active_dt).closed_hours
    day_compacted: dict[str, bool] = {}
    missing: list[str] = []
    compacted_skips = 0
    if observation is not None:
        observation.heal_horizon_hours, observation.heal_candidates = heal_cap, len(candidates)
        if observation.closed_hours is not None:
            observation.outside_heal_horizon_hours = max(0, observation.closed_hours - len(candidates))
    for hour in candidates:
        if hour in plan.bundled_hours or hour in plan.per_field_hours_seen:
            continue
        day = hour[:10]
        # A boundary day's aggregate was not selected: its retired hourly
        # inputs still need scoped raw healing, not a successful partial result.
        if day in plan.days_with_day_files_any and win.day_fully_in_window(day):
            if day not in day_compacted:
                day_compacted[day] = _day_has_real_rollup(plan, safe_fields, day)
            if day_compacted[day]:
                compacted_skips += 1
                continue
        missing.append(hour)
    if observation is not None:
        observation.heal_missing_hours, observation.compacted_hours_skipped = len(missing), compacted_skips
    return missing


def plan_top_n_paths(
    cache_dir: str, safe_fields: list[str], win: TopNWindow, *, observation: TopNRollupCoverage | None = None
) -> TopNPlan:
    """Enumerate the exact parquet files a top-N read needs for ``win``."""
    rollups = os.path.join(cache_dir, "rollups")
    day_root, hour_root, bundled_day_root = (
        os.path.join(rollups, "day"),
        os.path.join(rollups, "hour"),
        os.path.join(rollups, "day_bundled"),
    )
    days_with_day_files_any: set[str] = set()
    fields_with_day_dirs: set[str] = set()
    covered_by_any_field = _day_covered_by_any_field(
        day_root, safe_fields, win, days_with_day_files_any, fields_with_day_dirs=fields_with_day_dirs
    )
    bundled_day_paths, bundled_days = _bundled_day_files(bundled_day_root, win, days_with_day_files_any)
    bundled_hour_paths, bundled_hours = _bundled_hour_files(
        os.path.join(rollups, "hour_bundled"), win, bundled_days, covered_by_any_field
    )
    paths = TopNPaths(bundled_hour_paths=bundled_hour_paths, bundled_day_paths=bundled_day_paths)
    hours_seen = _per_field_files(
        safe_fields,
        day_root,
        hour_root,
        win,
        bundled_days,
        bundled_hours,
        paths,
        fields_with_day_dirs=fields_with_day_dirs,
    )
    if observation is not None:
        observation.active_hour_start = win.active_dt.isoformat()
        observation.bundled_day_files, observation.bundled_hour_files = (
            len(paths.bundled_day_paths),
            len(paths.bundled_hour_paths),
        )
        observation.per_field_day_files, observation.per_field_hour_files = len(paths.day_paths), len(paths.hour_paths)
        selected_days = bundled_days | {
            os.path.basename(os.path.dirname(path)).removeprefix("day=") for path in paths.day_paths
        }
        observation.any_field_covered_hours = len(selected_days) * 24 + len(
            {hour for hour in bundled_hours | hours_seen if hour[:10] not in selected_days}
        )
        if win.st_dt is not None and win.et_dt is not None:
            start = win.st_dt.replace(minute=0, second=0, microsecond=0)
            observation.closed_hours = max(0, math.ceil((min(win.et_dt, win.active_dt) - start).total_seconds() / 3600))
    return TopNPlan(paths, bundled_hours, bundled_days, days_with_day_files_any, hours_seen, day_root, bundled_day_root)


@dataclass
class IpSpreadPlan:
    """Files covering one window, in the two layouts the writer produces."""

    bundled_paths: list[str] = dc_field(default_factory=list)
    bundled_hours: set[str] = dc_field(default_factory=set)
    per_field_paths: list[str] = dc_field(default_factory=list)
    per_field_hours_by_field: dict[str, set[str]] = dc_field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.bundled_paths or self.per_field_paths)


def plan_ip_spread_paths(
    bundled_hour_root: str, per_field_root: str, safe_fields: list[str], win: TopNWindow
) -> IpSpreadPlan | None:
    """Plan an ip-spread read of ``win``, or ``None`` if neither tree exists yet."""
    if not os.path.isdir(per_field_root) and not os.path.isdir(bundled_hour_root):
        return None
    plan = IpSpreadPlan(per_field_hours_by_field={f: set() for f in safe_fields})
    if os.path.isdir(bundled_hour_root):
        with contextlib.suppress(OSError):
            for entry in os.listdir(bundled_hour_root):
                if entry.startswith("hour=") and win.hour_in_range(hour := entry[len("hour=") :]):
                    bundle_path = os.path.join(bundled_hour_root, entry, IP_SPREAD_BUNDLE_FILENAME)
                    if os.path.isfile(bundle_path):
                        plan.bundled_paths.append(bundle_path)
                        plan.bundled_hours.add(hour)
    if os.path.isdir(per_field_root):
        wanted = set(safe_fields)
        with contextlib.suppress(OSError):
            for field_entry in os.listdir(per_field_root):
                if not field_entry.startswith("field=") or (fld := field_entry[len("field=") :]) not in wanted:
                    continue
                field_dir = os.path.join(per_field_root, field_entry)
                with contextlib.suppress(OSError):
                    for hour_entry in os.listdir(field_dir):
                        if not hour_entry.startswith("hour="):
                            continue
                        hour = hour_entry[len("hour=") :]
                        if not win.hour_in_range(hour) or hour in plan.bundled_hours:
                            continue
                        hour_dir = os.path.join(field_dir, hour_entry)
                        for fname in os.listdir(hour_dir):
                            if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                                plan.per_field_paths.append(os.path.join(hour_dir, fname))
                                plan.per_field_hours_by_field[fld].add(hour)
    return plan


def build_ip_spread_sql(plan: IpSpreadPlan, safe_fields: list[str]) -> str:
    """UNION ALL of at most two SELECT branches, filtered to the requested fields."""
    field_filter = "(" + ", ".join("'" + escape_sql_literal(f) + "'" for f in safe_fields) + ")"
    select = "SELECT field, value, ip_sketch, sample_capped "
    branches: list[str] = [
        f"{select}FROM read_parquet([{quote_path_list(paths)}], hive_partitioning={hp}) WHERE field IN {field_filter}"
        for paths, hp in ((plan.bundled_paths, 0), (plan.per_field_paths, 1))
        if paths
    ]
    return " UNION ALL ".join(f"({b})" for b in branches)


def fold_sketches(
    rows: Iterable[tuple[str, str, object, object]],
) -> tuple[dict[tuple[str, str], HyperLogLog], dict[tuple[str, str], bool]]:
    """Merge the per-hour sketches by ``(field, value)``."""
    grouped_blobs: dict[tuple[str, str], list[bytes | bytearray]] = {}
    grouped_precision: dict[tuple[str, str], int] = {}
    any_capped: dict[tuple[str, str], bool] = {}
    for fld, value, sketch_bytes, capped in rows:
        if not isinstance(sketch_bytes, (bytes, bytearray)) or len(sketch_bytes) < 2:
            continue
        version, precision = sketch_bytes[0], sketch_bytes[1]
        if version != SERIALIZATION_VERSION or not (4 <= precision <= 16) or len(sketch_bytes) != 2 + (1 << precision):
            continue
        key = (fld, value)
        if key not in grouped_blobs:
            grouped_precision[key] = precision
            grouped_blobs[key] = [sketch_bytes]
            any_capped[key] = bool(capped)
        else:
            any_capped[key] = any_capped[key] or bool(capped)
            if precision == grouped_precision[key]:
                grouped_blobs[key].append(sketch_bytes)
    merged: dict[tuple[str, str], HyperLogLog] = {
        key: hll
        for key, blobs in grouped_blobs.items()
        if (hll := HyperLogLog.merge_bytes(blobs, precision=grouped_precision[key])) is not None
    }
    return merged, any_capped


def spread_meta(safe_fields: list[str], plan: IpSpreadPlan, any_capped: dict[tuple[str, str], bool]) -> dict[str, dict]:
    """Per-field coverage hints so callers need not re-walk the filesystem."""
    capped_by_field: dict[str, int] = {f: 0 for f in safe_fields}
    for (fld, _value), capped in any_capped.items():
        if capped:
            capped_by_field[fld] = capped_by_field.get(fld, 0) + 1
    return {
        f: {
            "coverage_hours": len(plan.bundled_hours | plan.per_field_hours_by_field.get(f, set())),
            "capped_values": capped_by_field.get(f, 0),
        }
        for f in safe_fields
    }


_SchemaSeeds = SchemaSeeds
_TopNWindow = TopNWindow
_build_ip_spread_sql = build_ip_spread_sql
_build_rolled_sql = build_rolled_sql
_find_unrolled_hours = find_unrolled_hours
_fold_rows = fold_rows
_fold_sketches = fold_sketches
_live_topn_fields = live_topn_fields
_plan_ip_spread_paths = plan_ip_spread_paths
_plan_top_n_paths = plan_top_n_paths
_spread_meta = spread_meta
_truncate_top_n = truncate_top_n
