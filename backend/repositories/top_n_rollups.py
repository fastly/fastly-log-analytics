"""Small units behind ``QueryRunner.execute_top_n_rollups``.

The method used to be one ~830-line body. It is now an orchestrator over the
pure pieces here: choosing which fields get a live top-up, folding the rolled /
live / heal row sets together, and truncating each field to its own limit. The
path planning and SQL assembly live next to them as they are extracted.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any

from backend.core.rollups._common import quote_path_list
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
