"""Small units behind ``QueryRunner.execute_top_n_rollups``.

The method used to be one ~830-line body. It is now an orchestrator over the
pure pieces here: choosing which fields get a live top-up, folding the rolled /
live / heal row sets together, and truncating each field to its own limit. The
path planning and SQL assembly live next to them as they are extracted.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Mapping
from typing import Any

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
        for field, value, count in rows:
            bucket = by_field.setdefault(field, {})
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
