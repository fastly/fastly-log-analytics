"""SQL templates for `backend.repositories.performance`.

Phase 5a extraction. Per-template inputs documented inline; non-trusted
values are bound via DuckDB ``?`` parameters, never interpolated.

See ``pending-docs/sql_ownership_audit.md`` for the migration shape.
"""

from __future__ import annotations

# ── Origin time-series ────────────────────────────────────────────────────────

ORIGIN_TIMESERIES = (
    "SELECT {time_bucket_select},\n"
    "       {value_expr} AS value\n"
    "FROM {table}\n"
    "WHERE {where_clause} AND \"{metric_col}\" IS NOT NULL\n"
    "GROUP BY 1 ORDER BY 1"
)
"""Per-bucket origin latency time series at a percentile of choice.

Inputs (all trusted-identifier substitutions):
- ``{time_bucket_select}`` — output of ``_base.time_bucket_select(interval)``
- ``{value_expr}`` — pre-built percentile expression (microseconds or
  seconds depending on ``metric_col``); see callers for the shape
- ``{table}`` — quoted base table identifier
- ``{where_clause}`` — pre-built WHERE clause from ``build_where_clause``
- ``{metric_col}`` — column name (e.g. ``ottfb``, ``ottlb``, ``ttfb``)

Output: ``(bucket_timestamp, value_ms)`` per row.

User input (window bounds, filter values) is bound through the
``runner.execute_with_retry`` ``params`` argument, never interpolated.
"""

__all__ = ["ORIGIN_TIMESERIES"]
