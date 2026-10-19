"""Units behind ``QueryRunner.execute_ip_spread_rollups``.

Planning which ip-spread parquets cover a window, assembling the read SQL,
folding the per-hour HyperLogLog sketches, and shaping the per-field coverage
metadata. The active hour is never read from rollups; the caller's live scan
covers it.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass, field

from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME, quote_path_list
from backend.repositories.top_n_rollups import TopNWindow
from backend.utils.hll import HyperLogLog
from backend.utils.sql_validator import escape_sql_literal


@dataclass
class IpSpreadPlan:
    """Files covering one window, in the two layouts the writer produces.

    The bundled tree (one multi-field file per hour) wins: per-field files for
    an hour already covered by a bundle are skipped so nothing is counted twice.
    The trees differ in hive layout (``field`` is a column in the bundle, a
    path segment per-field), so each is its own ``read_parquet`` branch.
    """

    bundled_paths: list[str] = field(default_factory=list)
    bundled_hours: set[str] = field(default_factory=set)
    per_field_paths: list[str] = field(default_factory=list)
    per_field_hours_by_field: dict[str, set[str]] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.bundled_paths or self.per_field_paths)


def plan_ip_spread_paths(
    bundled_hour_root: str, per_field_root: str, safe_fields: list[str], win: TopNWindow
) -> IpSpreadPlan | None:
    """Plan an ip-spread read of ``win``, or ``None`` if neither tree exists yet.

    ``None`` means a cold pool (no writer tick has run). An existing but empty
    plan is returned as a falsy :class:`IpSpreadPlan` so the caller can tell
    "no tree" from "tree without in-window files"; both mean "fall back live".
    """
    if not os.path.isdir(per_field_root) and not os.path.isdir(bundled_hour_root):
        return None
    plan = IpSpreadPlan(per_field_hours_by_field={f: set() for f in safe_fields})
    if os.path.isdir(bundled_hour_root):
        try:
            for entry in os.listdir(bundled_hour_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[len("hour=") :]
                if not win.hour_in_range(hour):
                    continue
                bundle_path = os.path.join(bundled_hour_root, entry, IP_SPREAD_BUNDLE_FILENAME)
                if os.path.isfile(bundle_path):
                    plan.bundled_paths.append(bundle_path)
                    plan.bundled_hours.add(hour)
        except OSError:
            pass
    if os.path.isdir(per_field_root):
        wanted = set(safe_fields)
        try:
            for field_entry in os.listdir(per_field_root):
                if not field_entry.startswith("field="):
                    continue
                fld = field_entry[len("field=") :]
                if fld not in wanted:
                    continue
                field_dir = os.path.join(per_field_root, field_entry)
                try:
                    for hour_entry in os.listdir(field_dir):
                        if not hour_entry.startswith("hour="):
                            continue
                        hour = hour_entry[len("hour=") :]
                        if not win.hour_in_range(hour) or hour in plan.bundled_hours:
                            continue
                        hour_dir = os.path.join(field_dir, hour_entry)
                        for fname in os.listdir(hour_dir):
                            if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                                continue
                            plan.per_field_paths.append(os.path.join(hour_dir, fname))
                            plan.per_field_hours_by_field[fld].add(hour)
                except OSError:
                    continue
        except OSError:
            pass
    return plan


def build_ip_spread_sql(plan: IpSpreadPlan, safe_fields: list[str]) -> str:
    """UNION ALL of at most two SELECT branches, filtered to the requested fields."""
    field_filter = "(" + ", ".join("'" + escape_sql_literal(f) + "'" for f in safe_fields) + ")"
    select = "SELECT field, value, ip_sketch, sample_capped "
    branches: list[str] = []
    if plan.bundled_paths:
        branches.append(
            f"{select}FROM read_parquet([{quote_path_list(plan.bundled_paths)}], hive_partitioning=0) "
            f"WHERE field IN {field_filter}"
        )
    if plan.per_field_paths:
        branches.append(
            f"{select}FROM read_parquet([{quote_path_list(plan.per_field_paths)}], hive_partitioning=1) "
            f"WHERE field IN {field_filter}"
        )
    return " UNION ALL ".join(f"({b})" for b in branches)


def fold_sketches(
    rows: Iterable[tuple[str, str, object, object]],
) -> tuple[dict[tuple[str, str], HyperLogLog], dict[tuple[str, str], bool]]:
    """Merge the per-hour sketches by ``(field, value)``.

    Returns the merged sketches and, per key, whether any input was flagged
    ``sample_capped``. HLL merge is O(m) = O(256) per pair so cost scales with
    rows pulled. Rows with a missing or malformed blob are skipped (on-disk
    corruption the next recompute overwrites), as are precision mismatches
    (unexpected: the writer pins p=8); partial beats wrong.
    """
    merged: dict[tuple[str, str], HyperLogLog] = {}
    any_capped: dict[tuple[str, str], bool] = {}
    for fld, value, sketch_bytes, capped in rows:
        if not isinstance(sketch_bytes, (bytes, bytearray)) or not sketch_bytes:
            continue
        try:
            hll = HyperLogLog.from_bytes(sketch_bytes)
        except ValueError:
            continue
        key = (fld, value)
        if key not in merged:
            merged[key] = HyperLogLog(precision=hll.precision)
            any_capped[key] = bool(capped)
        else:
            any_capped[key] = any_capped[key] or bool(capped)
        try:
            merged[key].merge(hll)
        except ValueError:
            continue
    return merged, any_capped


def spread_meta(safe_fields: list[str], plan: IpSpreadPlan, any_capped: dict[tuple[str, str], bool]) -> dict[str, dict]:
    """Per-field coverage hints so callers need not re-walk the filesystem.

    ``coverage_hours`` is the union of bundle-covered hours (which hold every
    field) and the field's own per-field hours; ``capped_values`` counts the
    ``(field, value)`` pairs any input flagged as capped (a lower bound).
    """
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
