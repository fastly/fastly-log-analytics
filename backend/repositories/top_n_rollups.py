"""Small units behind ``QueryRunner.execute_top_n_rollups``.

The method used to be one ~830-line body. It is now an orchestrator over the
pure pieces here: choosing which fields get a live top-up, folding the rolled /
live / heal row sets together, and truncating each field to its own limit. The
path planning and SQL assembly live next to them as they are extracted.
"""

from __future__ import annotations

import heapq
import math
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import datetime, timedelta
from typing import Any

from backend.core.rollups._common import quote_path_list
from backend.models.reader_coverage import TopNRollupCoverage
from backend.repositories.rollup_coverage import CoverageIndex, cached_listdir
from backend.utils.date_utils import parse_day_key_strict
from backend.utils.sql_validator import escape_sql_literal

TopNRow = tuple[str, Any, int]

_EDGE_PREFIXES = ("cmcd_", "edge_", "rum_")


def live_topn_fields(
    safe_fields: Iterable[str],
    skip_fields: frozenset[str],
    log_fields: Mapping[str, Any] | None,
) -> list[str]:
    """Fields that get a live (non-rolled) top-up, in ``safe_fields`` order.

    Drops ``skip_fields`` (never rendered as a facet) and the optional
    ``cmcd_`` / ``edge_`` / ``rum_`` / ``fastly_req_id`` columns unless the
    service enabled them as a dashboard custom field.
    """
    custom_fields = {
        cf["name"]
        for cf in (log_fields or {}).get("custom_fields", [])
        if cf.get("enabled", True) and cf.get("show_in_dashboard", True)
    }
    out: list[str] = []
    for f in safe_fields:
        if f in skip_fields:
            continue
        if f.startswith(_EDGE_PREFIXES) or f == "fastly_req_id":
            if f not in custom_fields:
                continue
        out.append(f)
    return out


def fold_rows(*row_sets: Iterable[TopNRow]) -> dict[str, dict[Any, int]]:
    """Sum ``(field, value, count)`` rows from every set into one bucket per field.

    Bucketing by field once keeps the merge O(rows); a flat ``(field, value)``
    dict re-scanned per field was the single biggest phase inside the old
    reader.
    """
    by_field: dict[str, dict[Any, int]] = {}
    for rows in row_sets:
        for fld, value, count in rows:
            bucket = by_field.setdefault(fld, {})
            bucket[value] = bucket.get(value, 0) + count
    return by_field


def truncate_top_n(
    by_field: Mapping[str, Mapping[Any, int]],
    fields: Iterable[str],
    limit: int,
    per_field_limits: Mapping[str, int] | None,
) -> tuple[list[TopNRow], list[str]]:
    """Top rows per requested field, plus the requested fields with no rows.

    Each field is cut to ``per_field_limits.get(field, limit)``. ``heapq``
    avoids a full sort when slicing a small top-N out of a large bucket.
    Fields come out in request order; ``empty`` keeps request order too.
    """
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
        if field_limit < len(bucket):
            top_items = heapq.nlargest(field_limit, items, key=lambda x: x[1])
        else:
            top_items = sorted(items, key=lambda x: x[1], reverse=True)
        for val, count in top_items:
            top_results.append((field, val, count))
    return top_results, empty


@dataclass
class SchemaSeeds:
    """Lazily resolved column list + column types, shared by the live and heal steps.

    Callers that already paid for the schema lookup pass it in as the seed;
    otherwise the first step to need it fetches it and later steps reuse the
    result, so a request does at most one lookup of each.
    """

    cols_seed: list[str] | None = None
    types_seed: dict[str, str] | None = None
    cols: list[str] = dc_field(default_factory=list)
    types: dict[str, str] = dc_field(default_factory=dict)

    def ensure(self, get_cols: Callable[[], list[str]], get_types: Callable[[], dict[str, str]]) -> None:
        """Fill ``cols`` then ``types`` if still unset (columns first: a failing
        column lookup must not trigger the types lookup)."""
        if not self.cols:
            self.cols = self.cols_seed if self.cols_seed is not None else get_cols()
        if not self.types:
            self.types = self.types_seed if self.types_seed is not None else get_types()


@dataclass
class TopNPaths:
    """Parquet files one top-N read hands to DuckDB, one list per source shape.

    The four shapes live under different hive layouts (``day=`` / ``hour=`` in
    the path vs ``field`` as a regular column in the bundles), so each becomes
    its own ``read_parquet`` branch; mixing them in one call is a Binder Error.
    """

    day_paths: list[str] = dc_field(default_factory=list)
    hour_paths: list[str] = dc_field(default_factory=list)
    bundled_hour_paths: list[str] = dc_field(default_factory=list)
    bundled_day_paths: list[str] = dc_field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.day_paths or self.hour_paths or self.bundled_hour_paths or self.bundled_day_paths)


def qualify_limit_expr(safe_fields: Iterable[str], limit: int, per_field_limits: Mapping[str, int] | None) -> str:
    """SQL expression for the per-field ``QUALIFY`` cutoff.

    A ``CASE`` keyed on ``field`` so a ``per_field_limits`` override (e.g.
    ``{"country": 500}``) only widens the one field it targets; collapsing to
    ``max(all limits)`` made every other field's cutoff 500 too and tripled the
    query's cost (2026-08-03). Overrides for fields not being read are dropped.
    """
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
) -> str | None:
    """The rolled-parquet top-N query, or ``None`` when there is nothing to read.

    Each non-empty path list is its own ``read_parquet`` branch, UNION ALL'd so
    ``SUM(count)`` aggregates across sources. ``CAST(count AS BIGINT)``
    normalises the type (per-hour files store BIGINT, the compaction COPY
    writes DOUBLE). The bundles carry EVERY field's rows while the per-field
    branches are already field-scoped by the directory walk, so the bundle
    branches get a ``field IN (...)`` filter: it cannot prune IO but collapses
    the aggregation hash table and QUALIFY partitions to the requested fields.
    """
    if not paths:
        return None
    field_in_list = ", ".join("'" + escape_sql_literal(f) + "'" for f in safe_fields)
    select = "SELECT field, value, CAST(count AS BIGINT) AS count "
    branches: list[str] = []
    for per_field_tree in (paths.day_paths, paths.hour_paths):
        if per_field_tree:
            branches.append(f"{select}FROM read_parquet([{quote_path_list(per_field_tree)}], hive_partitioning=1)")
    for bundles in (paths.bundled_hour_paths, paths.bundled_day_paths):
        if bundles:
            branches.append(
                f"{select}FROM read_parquet([{quote_path_list(bundles)}], hive_partitioning=0) "
                f"WHERE field IN ({field_in_list})"
            )
    return (
        "SELECT field, value, SUM(count) AS c FROM ("
        + " UNION ALL ".join(branches)
        + ") GROUP BY field, value QUALIFY ROW_NUMBER() OVER "
        f"(PARTITION BY field ORDER BY c DESC) <= {qualify_limit_expr(safe_fields, limit, per_field_limits)}"
    )


@dataclass(frozen=True)
class TopNWindow:
    """One request's window in the hour/day keys the rollup tree is named by.

    ``st_floor`` / ``et_floor`` are the inclusive ``YYYY-MM-DD-HH`` bounds the
    directory walk compares against (fixed-width, so a string compare is
    correct). ``et_floor`` is taken 1 µs before ``et`` so a request ending
    exactly on an hour boundary excludes that hour, matching the half-open
    ``timestamp < et`` the live queries use.
    """

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
        """Whether a per-day file's whole 24 h lies inside the request window.

        A boundary day's file would over-count rows outside the window, so such
        days fall back to their per-hour rollups.
        """
        try:
            day_start = parse_day_key_strict(day_str)
        except ValueError:
            return False
        day_end = day_start + timedelta(days=1)
        if self.st_dt and self.st_dt > day_start:
            return False
        return not (self.et_dt and self.et_dt < day_end)

    def day_in_range(self, day: str) -> bool:
        """Day key is not before the first / after the last window day."""
        if self.st_floor and day < self.st_floor[:10]:
            return False
        return not (self.et_floor and day > self.et_floor[:10])

    def hour_in_range(self, hour: str) -> bool:
        """Closed hour key inside ``[st_floor, et_floor]`` (the active hour is excluded)."""
        if self.st_floor and hour < self.st_floor:
            return False
        if self.et_floor and hour > self.et_floor:
            return False
        return hour < self.active_str


@dataclass
class TopNPlan:
    """What the filesystem walk found for one window.

    ``bundled_hours`` / ``per_field_hours_seen`` / ``days_with_day_files_any``
    are the coverage facts the missing-hour heal needs afterwards; ``paths``
    is what the rolled query reads.
    """

    paths: TopNPaths
    bundled_hours: set[str]
    bundled_days: set[str]
    days_with_day_files_any: set[str]
    per_field_hours_seen: set[str]
    day_root: str
    bundled_day_root: str


def _has_real_parquet(directory: str) -> bool:
    return any(f.endswith(".parquet") and not f.startswith(".tmp_") for f in cached_listdir(directory))


def _day_covered_by_any_field(
    day_root: str, safe_fields: list[str], win: TopNWindow, days_with_day_files_any: set[str]
) -> set[str]:
    """Closed, fully-in-window days where at least one field has a usable day file.

    Bundled-hour files for these days are skipped (they would double-count the
    covered fields); fields without a day file for the day fall through to
    their own per-field per-hour walk. Also records every day token with ANY
    day-rollup presence in ``days_with_day_files_any`` for the heal.
    """
    covered: set[str] = set()
    active_day = win.active_str[:10]
    for field in safe_fields:
        field_day_dir = os.path.join(day_root, f"field={field}")
        if not os.path.isdir(field_day_dir):
            continue
        for day_entry in cached_listdir(field_day_dir):
            if not day_entry.startswith("day="):
                continue
            day = day_entry[len("day=") :]
            if len(day) != 10:
                continue
            days_with_day_files_any.add(day)
            if day in covered or day >= active_day or not win.day_in_range(day):
                continue
            if not win.day_fully_in_window(day):
                continue
            if _has_real_parquet(os.path.join(field_day_dir, day_entry)):
                covered.add(day)
    return covered


def _bundled_day_files(
    bundled_day_root: str, win: TopNWindow, days_with_day_files_any: set[str]
) -> tuple[list[str], set[str]]:
    """Per-day bundles (one parquet holding every field) usable for this window.

    Accepts ``day=D/all_fields.parquet``, ``day=D/day_bundle_D.parquet`` and the
    flat ``day_bundle_D.parquet`` spellings. Active and boundary days are
    skipped; boundary days read their in-window hours instead.
    """
    paths: list[str] = []
    days: set[str] = set()
    if not os.path.isdir(bundled_day_root):
        return paths, days
    active_day = win.active_str[:10]
    for entry in cached_listdir(bundled_day_root):
        b_day: str | None = None
        bundle_path: str | None = None
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
        if b_day >= active_day or not win.day_in_range(b_day):
            continue
        if not win.day_fully_in_window(b_day):
            continue
        if b_day in days:
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
        if not win.hour_in_range(hour):
            continue
        if hour[:10] in bundled_days or hour[:10] in day_covered_by_any_field:
            # A day bundle / a per-field day file covers this hour; reading the
            # hour bundle too would double-count through UNION ALL.
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
) -> set[str]:
    """Per-field day files, then per-field hour files for what they don't cover.

    Appends to ``out.day_paths`` / ``out.hour_paths`` and returns the hour keys
    served by at least one per-field hour file (the pre-bundle intermediate
    state, which counts as rollup coverage for the heal).
    """
    active_day = win.active_str[:10]
    active_str, st_floor, et_floor = win.active_str, win.st_floor, win.et_floor
    hours_seen: set[str] = set()
    for field in safe_fields:
        field_hour_dir = os.path.join(hour_root, f"field={field}")
        field_day_dir = os.path.join(day_root, f"field={field}")
        if not os.path.isdir(field_hour_dir):
            continue
        # A day bundle suppresses this field's day file AND its per-hour
        # fallback for that day (the bundle already aggregates the whole day).
        covered_days: set[str] = set(bundled_days)
        if os.path.isdir(field_day_dir):
            for day_entry in cached_listdir(field_day_dir):
                if not day_entry.startswith("day="):
                    continue
                day = day_entry[len("day=") :]
                if len(day) != 10:
                    continue
                if day in bundled_days or day >= active_day or not win.day_in_range(day):
                    continue
                if not win.day_fully_in_window(day):
                    continue
                day_dir = os.path.join(field_day_dir, day_entry)
                for fname in cached_listdir(day_dir):
                    if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                        out.day_paths.append(os.path.join(day_dir, fname))
                        covered_days.add(day)
        for hour_entry in cached_listdir(field_hour_dir):
            if not hour_entry.startswith("hour="):
                continue
            hour = hour_entry[len("hour=") :]
            # Inlined win.hour_in_range: this is the innermost loop (fields x hours)
            # and a method call per entry measurably slows the cold walk.
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
    """Whether ``day`` really was day-compacted (a bundle, or a field day dir holding a parquet).

    A dir entry alone isn't proof: a crashed compaction can leave an empty day
    dir, and treating that as compacted would permanently exclude the day from
    the heal, re-creating the silent undercount the heal exists to close.
    """
    if os.path.isfile(os.path.join(plan.bundled_day_root, f"day={day}", "all_fields.parquet")):
        return True
    for field in safe_fields:
        day_dir = os.path.join(plan.day_root, f"field={field}", f"day={day}")
        if os.path.isdir(day_dir) and _has_real_parquet(day_dir):
            return True
    return False


def find_unrolled_hours(
    plan: TopNPlan,
    win: TopNWindow,
    safe_fields: list[str],
    heal_cap: int,
    *,
    observation: TopNRollupCoverage | None = None,
) -> list[str]:
    """Closed in-window hours with no rollup coverage, which the caller live-queries.

    Looks back at most ``heal_cap`` hours from the active hour. An hour is
    covered by an hour bundle (a verified-empty sentinel bundle counts), a
    per-field hour file, or its day having been compacted: day compaction
    deletes the per-hour files by design, so a window cutting through such a
    day keeps its fast-but-partial read instead of paying a day scan per
    request. Day-compaction is verified lazily, only for candidate days.
    """
    floor = win.active_dt - timedelta(hours=heal_cap)
    if win.st_dt and win.st_dt > floor:
        floor = win.st_dt
    candidates = CoverageIndex.build(floor, win.et_dt or win.active_dt, now=win.active_dt).closed_hours
    day_compacted: dict[str, bool] = {}
    missing: list[str] = []
    compacted_skips = 0
    if observation is not None:
        observation.heal_horizon_hours = heal_cap
        observation.heal_candidates = len(candidates)
        if observation.closed_hours is not None:
            observation.outside_heal_horizon_hours = max(0, observation.closed_hours - len(candidates))
    for hour in candidates:
        if hour in plan.bundled_hours or hour in plan.per_field_hours_seen:
            continue
        day = hour[:10]
        if day in plan.days_with_day_files_any:
            if day not in day_compacted:
                day_compacted[day] = _day_has_real_rollup(plan, safe_fields, day)
            if day_compacted[day]:
                compacted_skips += 1
                continue
        missing.append(hour)
    if observation is not None:
        observation.heal_missing_hours = len(missing)
        observation.compacted_hours_skipped = compacted_skips
    return missing


def plan_top_n_paths(
    cache_dir: str,
    safe_fields: list[str],
    win: TopNWindow,
    *,
    observation: TopNRollupCoverage | None = None,
) -> TopNPlan:
    """Enumerate the exact parquet files a top-N read needs for ``win``.

    Preference per day: bundled day file, else per-field day files, else the
    hour bundle, else per-field hour files; the active hour/day is never read
    from rollups. Files are enumerated per (field, hour) in Python rather than
    globbed, because a ``**`` glob makes DuckDB stat every file in the tree
    before the WHERE can prune (2.8 s on 18,648 files, 2026-06-04).
    """
    rollups = os.path.join(cache_dir, "rollups")
    day_root = os.path.join(rollups, "day")
    hour_root = os.path.join(rollups, "hour")
    bundled_day_root = os.path.join(rollups, "day_bundled")
    days_with_day_files_any: set[str] = set()
    covered_by_any_field = _day_covered_by_any_field(day_root, safe_fields, win, days_with_day_files_any)
    bundled_day_paths, bundled_days = _bundled_day_files(bundled_day_root, win, days_with_day_files_any)
    bundled_hour_paths, bundled_hours = _bundled_hour_files(
        os.path.join(rollups, "hour_bundled"), win, bundled_days, covered_by_any_field
    )
    paths = TopNPaths(bundled_hour_paths=bundled_hour_paths, bundled_day_paths=bundled_day_paths)
    hours_seen = _per_field_files(safe_fields, day_root, hour_root, win, bundled_days, bundled_hours, paths)
    if observation is not None:
        observation.active_hour_start = win.active_dt.isoformat()
        observation.bundled_day_files = len(paths.bundled_day_paths)
        observation.bundled_hour_files = len(paths.bundled_hour_paths)
        observation.per_field_day_files = len(paths.day_paths)
        observation.per_field_hour_files = len(paths.hour_paths)
        selected_days = bundled_days | {
            os.path.basename(os.path.dirname(path)).removeprefix("day=") for path in paths.day_paths
        }
        observation.any_field_covered_hours = len(selected_days) * 24 + len(
            {hour for hour in bundled_hours | hours_seen if hour[:10] not in selected_days}
        )
        if win.st_dt is not None and win.et_dt is not None:
            start = win.st_dt.replace(minute=0, second=0, microsecond=0)
            end = min(win.et_dt, win.active_dt)
            observation.closed_hours = max(0, math.ceil((end - start).total_seconds() / 3600))
    return TopNPlan(paths, bundled_hours, bundled_days, days_with_day_files_any, hours_seen, day_root, bundled_day_root)
