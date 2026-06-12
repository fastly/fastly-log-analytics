"""SQL templates for `backend.repositories._base`.

Phase 5a extraction. See ``pending-docs/sql_ownership_audit.md`` for the
mechanical recipe and ``backend/repositories/_sql/__init__.py`` for the
ownership policy.

``_base`` is the shared QueryRunner module — every repository routes
queries through it. The templates here are reused across repositories
either directly (``CANONICAL_METRICS``) or via the QueryRunner helpers
(top-N batch, time-series rollup branches).

All format-string placeholders are trusted-identifier substitutions only
(quoted table names, validated column names, pre-built clauses). User
input (filter values, page bounds) is bound through DuckDB ``?``
parameters at the ``runner.execute(...)`` call site, never interpolated.

What stays inline in ``_base.py``
---------------------------------

- ``SELECT count(*), min(timestamp), max(timestamp) FROM <table>`` and
  ``SELECT count(*) FROM <table>`` (``get_source_extent``) — one-liners
  whose only variable is the runtime table identifier.
- ``CREATE TEMP TABLE <name> AS SELECT <cols> FROM <src> WHERE <pred>``
  (``create_filtered_temp_table``) — one-liner assembled from a per-call
  column list; templatising it adds noise without buying any locality.
- ``DROP TABLE IF EXISTS <name>`` cleanup — one-liner.
- The three ``read_parquet([<paths>], hive_partitioning=N)`` branches
  in ``execute_top_n_rollups`` and the ``read_parquet([<paths>])`` clause
  in ``try_time_series_from_rollup`` — the parquet path list is
  inline-escaped (DuckDB has no ``?`` binding for path-array literals)
  so the branch ends up materialised from local variables either way.
- The buffer/hourly direct-read branches in
  ``_create_active_hour_temp_direct`` — single ``read_parquet('<glob>')``
  per branch with the same parquet-path constraint.
"""

from __future__ import annotations

# ── Canonical metric expressions ──────────────────────────────────────────────

CANONICAL_METRICS: dict[str, str] = {
    "hit_rate": "ROUND(COUNT(*) FILTER (WHERE {cache_col} IN ('HIT', 'HIT-STALE')) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "requests": "COUNT(*)",
    "avg_ttfb": "ROUND(AVG(ttfb) * 1000.0, 2)",
    "p95_ttfb": "ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttfb) * 1000.0, 2)",
    "5xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "4xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 400 AND status < 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "avg_resp_bytes": "ROUND(AVG(resp_bytes), 2)",
    "total_resp_bytes": "SUM(resp_bytes)",
    "throughput": "ROUND(COALESCE(MEDIAN(CASE WHEN ({cache_col} ILIKE 'HIT%%') AND {elapsed_col} > 0 THEN {resp_bytes_col} * 1e6 / NULLIF(CAST({elapsed_col} AS DOUBLE), 0) ELSE NULL END), 0), 2)",
    "req_size": "ROUND(COALESCE(MEDIAN(CAST({header_bytes_col} AS DOUBLE) + CAST({req_bytes_col} AS DOUBLE)), 0), 2)",
    "ttfb_ms": "ROUND(COALESCE(MEDIAN(CASE WHEN ttfb IS NOT NULL AND ttfb > 0 THEN ttfb * 1000.0 ELSE NULL END), 0), 2)",
}
"""Per-metric SQL expressions used across all repositories.

Some entries reference sub-placeholders (e.g. ``{cache_col}``) that the
caller resolves via ``resolve_col(...)`` before formatting against a
parent template. The ``%%`` in ``throughput`` is an escaped literal
``%`` for the eventual ``ILIKE 'HIT%'`` after the caller's outer
``str.format`` pass — preserved byte-for-byte from the historical
inline definition so dashboards keep matching the same cache states.
"""


# ── Time-series rollup metric expressions ─────────────────────────────────────

TS_ROLLUP_METRIC_SQL: dict[str, str] = {
    "requests": "CAST(SUM(requests) AS BIGINT)",
    "5xx": "ROUND(SUM(status_5xx) * 100.0 / NULLIF(SUM(requests), 0), 2)",
    "4xx": "ROUND(SUM(status_4xx) * 100.0 / NULLIF(SUM(requests), 0), 2)",
    "hit_rate": "ROUND(SUM(hits) * 100.0 / NULLIF(SUM(requests), 0), 2)",
}
"""Chart metrics the 1-minute time-series rollup can serve directly.

Keys MUST match the ``ChartMetric`` Literal in
``backend/models/dashboard.py``. Each numerator/denominator pair has to
produce the same numeric value as its raw-row counterpart in
``CANONICAL_METRICS`` so rollup-served and raw-served buckets stay
consistent across the active-hour split.

Percentile / median metrics (p50/p95/p99 latency, throughput, req_size,
ttfb median) are excluded — they require sketch-based re-aggregation
which DuckDB doesn't ship — and fall through to the raw scan.
"""


LIVE_METRIC_SQL_FROM_RAW: dict[str, str] = {
    "requests": "COUNT(*)",
    "5xx": "ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "4xx": "ROUND(COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "hit_rate": "ROUND(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) * 100.0 / NULLIF(COUNT(*), 0), 2)",
}
"""Raw-row counterparts of ``TS_ROLLUP_METRIC_SQL``.

Used by ``QueryRunner.try_time_series_from_rollup`` when the requested
window crosses the active hour: the live slice produces buckets that
align numerically with the rollup-served buckets only if the same
aggregation shape runs on the raw rows.
"""


# ── Top-N rollup outer aggregate wrapper ──────────────────────────────────────

TOP_N_ROLLUP_AGGREGATE = "SELECT field, value, SUM(count) AS c FROM ({branches_union_all}) GROUP BY field, value"
"""Outer aggregator that sums per-field counts across rollup branches.

Inputs (trusted-identifier substitution; user input bound elsewhere):

- ``{branches_union_all}`` — pre-built UNION ALL of one or more
  ``SELECT field, value, CAST(count AS BIGINT) AS count
  FROM read_parquet([<paths>], hive_partitioning=N)`` branches. The
  branches themselves stay inline in ``execute_top_n_rollups`` because
  the parquet path lists are not parameterisable.

Output (per row): ``(field: str, value: str, c: BIGINT)`` — one row per
unique ``(field, value)`` across all input branches.
"""


# ── Time-series rollup live (active-hour) clause ──────────────────────────────

TS_LIVE_CLAUSE = (
    "SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
    "       {metric_sql} AS value "
    "FROM {table_name} "
    "WHERE {where_clause} "
    "  AND timestamp >= TIMESTAMPTZ '{live_st_iso}+00:00' "
    "  AND timestamp <  TIMESTAMPTZ '{live_et_iso}+00:00' "
    "GROUP BY 1"
)
"""Live (active-hour) branch of the time-series rollup query.

Active hours aren't bundled by the time-series rollup writer (see
``backend.core.rollups.build_time_series_bundles``), so when the
requested window includes the current UTC hour we run a raw-table query
for ``[max(start, active_hour_start), end)`` and UNION ALL it with the
rollup-served portion so the chart is current to the second.

Inputs (all trusted-identifier substitutions; user-supplied filter
values are bound through the ``params`` arg at the
``runner.execute(...)`` call site):

- ``{interval}`` — validated bucket interval (``"1 minute"`` /
  ``"1 hour"`` / ``"1 day"``); allowlisted via ``safe_interval``
- ``{metric_sql}`` — entry from ``LIVE_METRIC_SQL_FROM_RAW``
- ``{table_name}`` — quoted base table identifier or temp-table name
- ``{where_clause}`` — pre-built filter clause from
  ``build_where_clause`` (the same one used by the rollup branch); user
  values are bound by ``?`` here, not interpolated
- ``{live_st_iso}`` — ISO-8601 naive UTC timestamp for the live-slice
  start (``max(window_start, active_hour_start)``); naive because the
  ``+00:00`` literal is concatenated outside the placeholder
- ``{live_et_iso}`` — ISO-8601 naive UTC timestamp for the live-slice
  end (``window_end``)

Output (per row): ``(bucket: TIMESTAMP, value: float | None)``.
"""


# ── Time-series rollup outer wrapper ──────────────────────────────────────────

TS_OUTER_WRAPPER = "SELECT out_bucket, value FROM ({unioned_clauses}) WHERE out_bucket IS NOT NULL ORDER BY 1"
"""Outer wrapper around the UNION ALL of rollup + live clauses.

Inputs:

- ``{unioned_clauses}`` — pre-built ``(rollup_clause) UNION ALL
  (live_clause)`` string (each clause already wrapped in parens by the
  caller). The rollup branch is built inline in
  ``try_time_series_from_rollup`` because its ``read_parquet([<paths>])``
  isn't parameterisable; the live branch comes from ``TS_LIVE_CLAUSE``.

The rollup and live windows don't overlap by construction (the rollup
cursor stops at ``active_hour_str``), so SUM-style metrics don't need an
outer aggregation — the wrapper just filters NULL buckets and sorts.

Output (per row): ``(bucket: TIMESTAMP, value: float | None)``.
"""


# ── Top-N batch per-field subquery ────────────────────────────────────────────

TOP_N_BATCH_PER_FIELD = """
                (SELECT '{field}' as field, {select_val} as value, count(*) as c
                FROM {table_name}
                WHERE {where_filter}
                GROUP BY 1, 2 ORDER BY 3 DESC LIMIT {limit})
            """
"""One per-field subquery in the UNION ALL produced by
``execute_top_n_batch``.

A single repository call asks for top-N over several fields at once
(country, status, asn, ja4, …). Rather than firing N separate queries,
the runner builds one UNION ALL of these per-field subqueries — DuckDB
plans the shared scan once and emits N grouped result sets that we
demux in Python by the ``field`` literal column.

Inputs (all trusted-identifier substitutions; user input bound via
``params`` at the ``runner.execute`` call site):

- ``{field}`` — bare field identifier inlined as a SQL string literal so
  the result rows can be demuxed by field. Validated by
  ``_is_safe_ident`` upstream.
- ``{select_val}`` — column projection expression. For VARCHAR columns
  this is the bare column name; for INT-aggregate fields (``ttl``,
  ``age``) it's ``CAST(CAST(ROUND(<col>) AS INTEGER) AS VARCHAR)`` to
  collapse floating-point jitter; otherwise ``CAST(<col> AS VARCHAR)``.
- ``{table_name}`` — quoted base table or temp-table name.
- ``{where_filter}`` — column-specific null/empty predicate (e.g.
  ``"col" IS NOT NULL AND "col" != ''`` for VARCHAR, just
  ``"col" IS NOT NULL`` otherwise).
- ``{limit}`` — integer top-N cap. The widest per-field cap dictates the
  raw fetch size; truncation to per-field caps happens in Python.

Output (per row): ``(field: str, value: str, c: BIGINT)``.
"""


__all__ = [
    "CANONICAL_METRICS",
    "TS_ROLLUP_METRIC_SQL",
    "LIVE_METRIC_SQL_FROM_RAW",
    "TOP_N_ROLLUP_AGGREGATE",
    "TS_LIVE_CLAUSE",
    "TS_OUTER_WRAPPER",
    "TOP_N_BATCH_PER_FIELD",
]
