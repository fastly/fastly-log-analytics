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


# ── Preview (POST /api/alerts/preview) ───────────────────────────────────────
# Placeholders are trusted: ``{table}`` is a ``_safe_table_name`` identifier, ``{group_sql}`` /
# ``{agg_sql}`` / ``{scope_filter}`` come from fixed code branches and the metric catalog, and
# every ``{...minutes...}`` / ``{days}`` value is an integer derived from the validated alert.

PREVIEW_SERIES = """
        SELECT {group_sql} as ts, {agg_sql} as val
        FROM {table}
        WHERE timestamp >= (SELECT max(timestamp) FROM {table}) - INTERVAL '{lookback_mins} minutes' {scope_filter}
        GROUP BY 1
        ORDER BY 1 ASC
    """

PREVIEW_ANY_ROW = "SELECT 1 FROM {table} LIMIT 1"

PREVIEW_MAX_TS = "SELECT max(timestamp) FROM {table}"

PREVIEW_BASELINE = """
                SELECT
                    EXTRACT(hour FROM ts) AS hr,
                    avg(val) AS mean_val,
                    stddev(val) AS stddev_val
                FROM (
                    SELECT date_trunc('hour', timestamp) AS ts, {agg_sql} AS val
                    FROM {table}
                    WHERE {baseline_where}
                    GROUP BY 1
                )
                GROUP BY 1
            """

PREVIEW_HISTORY = """
                 SELECT {group_sql} as ts, {agg_sql} as val
                 FROM {table}
                 WHERE timestamp >= (SELECT max(timestamp) FROM {table}) - INTERVAL '{start_minutes} minutes'
                   AND timestamp <= (SELECT max(timestamp) FROM {table}) - INTERVAL '{comp_period} minutes'
                   {scope_filter}
                 GROUP BY 1
                 ORDER BY 1 ASC
             """


__all__ = [
    "MAX_TIMESTAMP",
    "PREVIEW_SERIES",
    "PREVIEW_ANY_ROW",
    "PREVIEW_MAX_TS",
    "PREVIEW_BASELINE",
    "PREVIEW_HISTORY",
]
