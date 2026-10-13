"""Canonical SQL fragment builders (metric expressions).

A tiny typed catalog of SQL expression builders that were being re-derived,
byte-for-byte, across the rollup writers and the repository query layer. Each
builder takes the set of columns actually present on a service's schema
(``actual_cols``) and returns the exact SQL fragment to embed — so the
availability-adaptive logic lives in one place instead of being copy-pasted
with a "kept in sync rather than imported" comment at each site.

This module lives in ``backend.core`` (a leaf — stdlib only) precisely so the
rollup writers (``backend.core.rollups.*``) can import it without the
import-graph cycle that blocked importing the original helper out of
``backend.repositories._base`` (repositories depends on core, not the other
way around; import-linter's "Core does not depend on routers" and the
rollups→repositories cycle both stay satisfied).

Only expressions that were **byte-identical in meaning** across their call
sites are consolidated here. Fragments that merely look similar but differ in
cast, FILTER clause, quoting, or aggregation are intentionally left at their
call sites — collapsing them would be a behavior change, not DRY.
"""

from __future__ import annotations

from collections.abc import Collection

__all__ = ["origin_latency_us"]


def origin_latency_us(actual_cols: Collection[str], *, absent: str | None = "NULL") -> str | None:
    """Return the origin-latency-in-microseconds SQL expression for a schema.

    Resolves the three availability cases identically to every call site that
    previously inlined this fragment:

    * both ``ottfb`` (µs) and ``ttfb`` (s) present → prefer ``ottfb``, fall
      back to ``ttfb`` scaled to µs;
    * only ``ottfb`` → ``ottfb`` as-is;
    * only ``ttfb`` → ``ttfb`` scaled to µs.

    Args:
        actual_cols: Columns present on the service's schema.
        absent: Value to return when NEITHER latency column exists. Callers
            differ only here: the repository seed returns the literal string
            ``"NULL"`` (so the expression is always valid SQL), while the
            rollup writers pass ``absent=None`` to signal "skip this service —
            there is nothing to roll up".

    Returns:
        The SQL fragment string, or ``absent`` when no latency column exists.
    """
    has_ottfb = "ottfb" in actual_cols
    has_ttfb = "ttfb" in actual_cols
    if has_ottfb and has_ttfb:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    if has_ottfb:
        return '"ottfb"'
    if has_ttfb:
        return '"ttfb" * 1000000.0'
    return absent
