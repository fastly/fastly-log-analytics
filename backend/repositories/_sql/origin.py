"""SQL templates for `backend.repositories.origin`.

Phase 5a extraction. Per-template inputs documented inline; non-trusted
values are bound via DuckDB ``?`` parameters, never interpolated.

See ``backend/repositories/_sql/__init__.py`` for the ownership policy.

Each template is a Python ``str.format`` template. The format
placeholders are trusted-identifier substitutions only (table names,
column names, pre-built SQL fragments). User input (window bounds,
filter values, ``LIMIT``/``HAVING`` integers) is bound through the
``runner.execute`` ``params`` argument as ``?`` placeholders.
"""

from __future__ import annotations

# ── Live (non-temp) reads against the parquet-backed logs view ─────────────────

SUMMARY_ROLLUP = """
        SELECT
          COUNT(*)                                                                            AS requests,
          COUNT(*) FILTER (WHERE starts_with("cache", 'MISS'))                                       AS total_misses,
          COUNT(*) FILTER (WHERE starts_with("cache", 'PASS'))                                       AS total_passes,
          MEDIAN({lat_val}) / 1000.0                                                          AS ottfb_p50_ms,
          APPROX_QUANTILE({lat_val}, 0.75) / 1000.0                                           AS ottfb_p75_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                                           AS ottfb_p95_ms,
          APPROX_QUANTILE({lat_val}, 0.99) / 1000.0                                           AS ottfb_p99_ms,
          {ottlb_p50}                                                                          AS ottlb_p50_ms,
          {ottlb_p95}                                                                          AS ottlb_p95_ms,
          {cdn_ovh}                                                                            AS cdn_overhead_p50_ms,
          {ost_5xx}                                                                            AS origin_error_rate,
          {obytes_p50}                                                                         AS obytes_p50
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL)
        """
"""Rollup totals over the requested window — single () grouping pass.

The previous shape used ``GROUP BY GROUPING SETS ((), ("edge"))`` to
return overall totals AND a per-edge breakdown in one scan, but the
per-edge ``by_leg`` rows were never read by the frontend — the
/origin page hard-codes ``split_by_leg: false``. Dropping the per-edge
grouping cuts the second hash partition + the per-edge percentile
sorts; the remaining single-pass aggregate is roughly half the work.

Inputs (all trusted-identifier substitutions):
- ``{lat_val}`` — origin latency-us expression (e.g. ``COALESCE("ottfb", "ttfb"*1000000.0)``)
- ``{ottlb_p50}`` / ``{ottlb_p95}`` — ``"NULL"`` or a MEDIAN/APPROX_QUANTILE on ``"ottlb"``
- ``{cdn_ovh}`` — ``"NULL"`` or ``MEDIAN("elapsed" - "ottlb") / 1000.0``
- ``{ost_5xx}`` — ``"NULL"`` or origin-5xx error-rate expression
- ``{obytes_p50}`` — ``"NULL"`` or ``MEDIAN("obytes")``
- ``{table}`` — quoted base-table identifier (via ``_safe_table``)
- ``{where}`` — pre-built WHERE clause (uses ``?`` params)

Single output row:
``(requests, total_misses, total_passes,
   ottfb_p50_ms, ottfb_p75_ms, ottfb_p95_ms, ottfb_p99_ms,
   ottlb_p50_ms, ottlb_p95_ms, cdn_overhead_p50_ms,
   origin_error_rate, obytes_p50)``
"""


TIMESERIES_BUCKETED = """
        SELECT
          time_bucket({interval}, "timestamp")                              AS ts,
          COUNT(*)                                                          AS miss_count,
          {agg_expr} {unit_conv}                                            AS value
          {edge_col}
        FROM {table}
        WHERE {where} AND ({lat_expr} IS NOT NULL)
        GROUP BY ts {edge_group}
        ORDER BY ts
        """
"""Per-bucket origin latency time series, optionally split by edge leg.

Inputs (all trusted-identifier substitutions):
- ``{interval}`` — ``INTERVAL '<n>' seconds|minutes`` literal
- ``{agg_expr}`` — pre-built ``MEDIAN(...)`` or ``APPROX_QUANTILE(..., <p>)``
- ``{unit_conv}`` — ``"/ 1000.0"`` or ``"* 1000.0"`` for us->ms / s->ms conversion
- ``{edge_col}`` — ``', "edge"'`` when splitting, else ``""``
- ``{table}`` — quoted base-table identifier
- ``{where}`` — pre-built WHERE clause
- ``{lat_expr}`` — the latency expression matching ``agg_expr`` (e.g.
  ``COALESCE("ottfb", "ttfb"*1000000.0)`` or ``"ottfb"``)
- ``{edge_group}`` — ``', "edge"'`` when splitting, else ``""``

Output columns per row: ``(ts, miss_count, value[, edge])``
"""


SLOW_URLS = """
        SELECT
          "url",
          COUNT(*)                                                         AS requests,
          MEDIAN({lat_val}) / 1000.0                                       AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                        AS p95_ms,
          APPROX_QUANTILE({lat_val}, 0.99) / 1000.0                        AS p99_ms
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL) AND "url" IS NOT NULL
        GROUP BY "url"
        HAVING COUNT(*) >= ?
        ORDER BY p95_ms DESC
        LIMIT ?
        """
"""Top URLs by origin p95 latency, gated by a minimum-request count.

Inputs (trusted-identifier substitutions):
- ``{lat_val}`` — origin-latency expression
- ``{table}`` — quoted base-table identifier
- ``{where}`` — pre-built WHERE clause

The two ``?`` placeholders bind, in order: ``min_requests``, ``limit``.

Output columns per row: ``(url, requests, p50_ms, p95_ms, p99_ms)``
"""


STATUS_CODES = """
        SELECT
          CASE
            WHEN "ost" BETWEEN 100 AND 599 THEN "ost"
            ELSE -1
          END                                              AS status,
          COUNT(*)                                         AS count,
          COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()        AS pct
        FROM {table}
        WHERE {where} AND "ost" IS NOT NULL
        GROUP BY 1
        ORDER BY count DESC
        """
"""Origin status-code distribution.

N-8: bucket any non-standard status code (anything outside the 100-599
HTTP range) under a single ``-1`` sentinel that the frontend can map to
"Other". Origin logs occasionally surface synthetic values like 829 from
buggy backends or middlebox rewrites; renaming the donut to "HTTP 829"
implies it's a valid status that the user could investigate. Frontend at
``Timeseries.tsx`` translates -1 to "Other".

Inputs:
- ``{table}`` — quoted base-table identifier
- ``{where}`` — pre-built WHERE clause

Output columns per row: ``(status, count, pct)``. ``status == -1`` means
"non-standard HTTP code outside 100-599; bucketed".
"""


PATH_BREAKDOWN = """
        SELECT
          "edge",
          COUNT(*)                                                          AS requests,
          MEDIAN({lat_val}) / 1000.0                                        AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                         AS p95_ms
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL)
        GROUP BY "edge"
        """
"""Edge-vs-shield leg breakdown (one row per ``edge`` boolean).

Inputs:
- ``{lat_val}`` — origin-latency expression
- ``{table}`` — quoted base-table identifier
- ``{where}`` — pre-built WHERE clause

Output columns per row: ``(edge, requests, p50_ms, p95_ms)``
"""


POP_LATENCY = """
        SELECT
          "pop",
          COUNT(*)                                                          AS requests,
          MEDIAN({lat_val}) / 1000.0                                        AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                         AS p95_ms
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL) AND "pop" IS NOT NULL AND "pop" != ''
        GROUP BY "pop"
        ORDER BY p95_ms DESC
        LIMIT ?
        """
"""Top POPs by origin p95 latency.

Inputs:
- ``{lat_val}`` — origin-latency expression
- ``{table}`` — quoted base-table identifier
- ``{where}`` — pre-built WHERE clause

The trailing ``?`` placeholder binds ``limit``.

Output columns per row: ``(pop, requests, p50_ms, p95_ms)``
"""


IP_HEALTH = """
        SELECT
          "oip",
          COUNT(*)                                                            AS requests,
          MEDIAN({lat_val}) / 1000.0                                          AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                           AS p95_ms,
          ROUND(COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0
            / NULLIF(COUNT(*), 0), 1)                                         AS error_pct
        FROM {table}
        WHERE {where} AND "oip" IS NOT NULL AND "oip" != '' AND "ost" IS NOT NULL
        GROUP BY "oip"
        HAVING COUNT(*) >= 10
        ORDER BY error_pct DESC
        LIMIT ?
        """
"""Origin IPs ranked by 5xx error rate (min 10 requests/group).

Inputs:
- ``{lat_val}`` — origin-latency expression
- ``{table}`` — quoted base-table identifier
- ``{where}`` — pre-built WHERE clause

The trailing ``?`` placeholder binds ``limit``.

Output columns per row: ``(oip, requests, p50_ms, p95_ms, error_pct)``
"""


SHIELDING_ANALYSIS = """
        WITH edge_logs AS (
            SELECT "rid", "pop", "ottfb", "ttfb"
            FROM {table}
            WHERE {where} AND "edge" = true
        ),
        shield_logs AS (
            SELECT "prid", "pop", "ottfb", "ttfb"
            FROM {table}
            WHERE {time_where} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != ''
        ),
        pairs AS (
          SELECT
            e.pop                                                                    AS edge_pop,
            s.pop                                                                    AS shield_pop,
            COUNT(*)                                                                 AS requests,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (COALESCE(e.ottfb, e.ttfb * 1000000) - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p50_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (COALESCE(e.ottfb, e.ttfb * 1000000) - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p95_ms,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY (COALESCE(e.ottfb, e.ttfb * 1000000) - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p99_ms
          FROM edge_logs e
          INNER JOIN shield_logs s ON s.prid = e.rid
          GROUP BY 1, 2
        ),
        ranked AS (
          SELECT
            *,
            COUNT(*) OVER ()                                       AS total_routes,
            ROW_NUMBER() OVER (ORDER BY requests DESC)            AS rn_requests,
            ROW_NUMBER() OVER (ORDER BY p50_ms DESC NULLS LAST)  AS rn_overhead
          FROM pairs
        )
        SELECT edge_pop, shield_pop, requests, p50_ms, p95_ms, p99_ms, total_routes
        FROM ranked
        WHERE rn_requests <= ? OR rn_overhead <= ?
        ORDER BY requests DESC
    """
"""Edge<-shield POP pair latency analysis via self-join on ``rid``/``prid``.

The shield CTE intentionally drops user filters (only time bounds survive)
so an edge filter like ``pop = DEN`` doesn't strip the shield hit at IAD
before the join.

Route selection (M1, shielding audit 2026-06-30): a plain
``ORDER BY requests DESC LIMIT N`` structurally buried low-volume but
high-overhead routes — the exact mis-peered routes this analysis exists
to surface — and the ``anomaly_static`` flag was computed in Python AFTER
truncation, so a flagged route could be dropped before the operator ever
saw it. The ``ranked`` CTE now keeps the union of the top-N BY REQUESTS
*and* the top-N BY TRANSIT OVERHEAD (``p50_ms`` is the best in-SQL proxy
for "interesting"; the light-speed floor needs the Python POP map). The
``COUNT(*) OVER ()`` exposes the full distinct-pair count so the caller
can report "Top N of M" / set ``truncated`` instead of implying the table
is complete.

Inputs (trusted-identifier substitutions):
- ``{table}`` — quoted base-table identifier
- ``{where}`` — full WHERE clause (time + filters) applied to the edge CTE
- ``{time_where}`` — time-only WHERE clause applied to the shield CTE

Parameter binding order: ``edge_params + time_params + [limit, limit]``
(the two trailing ``?`` bind the by-requests and by-overhead rank cutoffs).

Output columns per row:
``(edge_pop, shield_pop, requests, p50_ms, p95_ms, p99_ms, total_routes)``.
At most ``2 * limit`` rows (the two rank sets when fully disjoint).
"""


# ── Composite endpoint: get_aggregates → CREATE TEMP TABLE + 8 reads ──────────

AGGREGATES_CREATE_TEMP = (
    "CREATE TABLE {temp_table} AS SELECT {select_cols}, {lat_us_expr} AS lat_us FROM {table} WHERE {where_clause}"
)
"""Materialise a per-request catalog table for the composite origin endpoint.

A regular catalog table is used (rather than explicit CREATE TEMP TABLE syntax)
to conform with standard naming conventions, but must be accessed sequentially
on the primary materializing connection. DuckDB temporary tables/views are
connection-scoped, so all downstream reads must execute sequentially on the same
primary pool connection to avoid CatalogExceptions.

Cleanup hygiene is two-layered: the get_aggregates ``try/finally``
DROPs the table on the normal exit path; the per-connection
catalog sweep in ``backend/core/duckdb_pool.py:_Pool.release``
mops up any ``t_*`` table left over by a kill / abandoned handler.

Computes the latency expression once at materialization time so the
downstream reads can sort/percentile on the ``lat_us`` column-store
column instead of paying per-row COALESCE during each percentile sort.

Inputs (all trusted-identifier substitutions):
- ``{temp_table}`` — generated table name (e.g. ``t_origin_<uuid>``)
- ``{select_cols}`` — comma-joined quoted-column list (e.g. ``'"timestamp", "cache"'``)
- ``{lat_us_expr}`` — origin-latency expression (becomes the ``lat_us`` column)
- ``{table}`` — quoted base-table identifier
- ``{where_clause}`` — pre-built WHERE clause with values inlined (no ``?``
  params — runner.create_temp_table uses ``inline_params=True``)
"""


# The TEMP-table mirror templates (TEMP_TIMESERIES / TEMP_SLOW_URLS /
# TEMP_STATUS_CODES / TEMP_PATH_BREAKDOWN / TEMP_POP_LATENCY /
# TEMP_IP_HEALTH / TEMP_SUMMARY_ROLLUP / TEMP_SUMMARY_BY_EDGE) were
# dropped — the live templates above already carry the
# ``{lat_val}`` / ``{table}`` / ``{where}`` placeholders we need for the
# per-request TEMP-table reads. Callers in origin.py render the live
# templates with ``table=<temp_table>``, ``where='1=1'``,
# ``lat_val='lat_us'``. The summary path uses SUMMARY_ROLLUP for
# both live and TEMP via :func:`backend.repositories.origin._shape_summary`,
# which switched to ``cursor.description``-based dict access so column
# additions can't silently shift downstream consumers (the b10 footgun).


__all__ = [
    "SUMMARY_ROLLUP",
    "TIMESERIES_BUCKETED",
    "SLOW_URLS",
    "STATUS_CODES",
    "PATH_BREAKDOWN",
    "POP_LATENCY",
    "IP_HEALTH",
    "SHIELDING_ANALYSIS",
    "AGGREGATES_CREATE_TEMP",
]
