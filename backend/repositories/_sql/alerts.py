"""SQL templates for `backend.repositories.alerts`.

Phase 5a extraction. See ``backend/repositories/_sql/__init__.py`` for the
ownership policy.

Each template is a Python format string. Format placeholders are
trusted-identifier substitutions only (table name, integer minutes
derived from the validated alert row). User input — operator,
threshold, status codes, etc. — never lands here; it stays in the
calling Python code where it's compared after the row is fetched.

The metric-query body itself (``SELECT <agg> FROM ... WHERE ...``) is
built at runtime by ``alerts._evaluate_alert``'s ``build_metric_query``
closure because its shape branches on:
  - whether the metric SQL (``backend.core.metrics.get_metric_sql``)
    is a bare aggregate vs. a full ``SELECT ... WHERE ...`` snippet,
  - the alert's ``evaluation_scope`` (all / edge / origin), and
  - whether the metric SQL already carries a ``WHERE`` clause.
That conditional shape doesn't lend itself to a single template — it
stays in the repository alongside the branching logic.
"""

from __future__ import annotations

# ── Standalone queries ───────────────────────────────────────────────────────

MAX_TIMESTAMP = "SELECT max(timestamp) FROM {table}"
"""Latest ingested log timestamp — used as the freshness gate and as the
anchor for every relative window expression below.

Inputs:
- ``{table}`` — trusted table identifier (result of ``_safe_table_name``).

Output (one row):
- column 0: ``TIMESTAMPTZ | None`` — ``None`` when the table is empty.
"""


COUNT_REQUESTS_IN_WINDOW = (
    "SELECT count(*) FROM {table} WHERE timestamp >= {window_start_expr} AND timestamp <= {window_end_expr}"
)
"""Total request count inside the alert's evaluation window.

Used to gate non-absolute alerts (``relative_increase`` /
``relative_decrease``) on a minimum-traffic floor before computing a
percent change — see ``evaluate_alert``.

Inputs (all trusted-identifier / pre-validated substitutions):
- ``{table}`` — trusted table identifier.
- ``{window_start_expr}`` — SQL expression for the window's lower bound;
  callers pass the result of ``WINDOW_OFFSET_EXPR.format(...)``.
- ``{window_end_expr}`` — SQL expression for the window's upper bound;
  callers pass the result of ``MAX_TIMESTAMP_SUBQUERY_EXPR.format(...)``.

Output (one row):
- column 0: ``BIGINT`` — request count (``0`` when the window is empty).
"""


# ── Window-bound subquery expressions ────────────────────────────────────────

MAX_TIMESTAMP_SUBQUERY_EXPR = "(SELECT max(timestamp) FROM {table})"
"""Parenthesised ``max(timestamp)`` subquery — embedded inside larger
queries (the count + the metric query) as the anchor for both the
current window's upper bound and the offsets below.

Inputs:
- ``{table}`` — trusted table identifier.

Renders to: ``(SELECT max(timestamp) FROM "<table>")`` — a scalar
expression suitable for arithmetic with ``INTERVAL`` literals.
"""


WINDOW_OFFSET_EXPR = "(SELECT max(timestamp) FROM {table}) - INTERVAL '{minutes_ago} minutes'"
"""Window-bound expression: ``max_ts - INTERVAL 'N minutes'``.

Used four times by ``evaluate_alert``:
- current-window start: ``minutes_ago = window``
- historic-window start: ``minutes_ago = comp_period + window``
- historic-window end: ``minutes_ago = comp_period``
(The current-window END is just ``MAX_TIMESTAMP_SUBQUERY_EXPR`` — no
offset.)

Inputs (both trusted):
- ``{table}`` — trusted table identifier.
- ``{minutes_ago}`` — non-negative integer derived from the validated
  alert row (``window_min`` / ``comparison_period_min``); never raw
  user input.

Renders to a scalar ``TIMESTAMPTZ`` expression suitable for
embedding inside a ``WHERE timestamp >= ...`` clause.
"""


__all__ = [
    "MAX_TIMESTAMP",
    "COUNT_REQUESTS_IN_WINDOW",
    "MAX_TIMESTAMP_SUBQUERY_EXPR",
    "WINDOW_OFFSET_EXPR",
]
