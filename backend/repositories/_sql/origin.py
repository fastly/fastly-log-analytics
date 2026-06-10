"""SQL templates for `backend.repositories.origin`.

Phase 5a extraction. Per-template inputs documented inline; non-trusted
values are bound via DuckDB ``?`` parameters, never interpolated.

See ``pending-docs/sql_ownership_audit.md`` for the migration shape and
``backend/repositories/_sql/__init__.py`` for the ownership policy.

Each template is a Python ``str.format`` template. The format
placeholders are trusted-identifier substitutions only (table names,
column names, pre-built SQL fragments). User input (window bounds,
filter values, ``LIMIT``/``HAVING`` integers) is bound through the
``runner.execute`` ``params`` argument as ``?`` placeholders.
"""

from __future__ import annotations

# ── Live (non-temp) reads against the parquet-backed logs view ─────────────────

SUMMARY_GROUPING_SETS = """
        SELECT
          {edge_select}                                                                       AS edge_group,
          {grouping_expr}                                                                     AS is_total,
          COUNT(*)                                                                            AS requests,
          COUNT(*) FILTER (WHERE "cache" ILIKE 'MISS%')                                       AS total_misses,
          COUNT(*) FILTER (WHERE "cache" ILIKE 'PASS%')                                       AS total_passes,
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
        {grouping_clause}
        """
"""Combined rollup totals + per-edge breakdown in a single scan.

Inputs (all trusted-identifier substitutions):
- ``{edge_select}`` — ``'"edge"'`` when the ``edge`` column exists, else ``"NULL"``
- ``{grouping_expr}`` — ``'GROUPING("edge")'`` when edge exists, else literal ``"1"``
- ``{lat_val}`` — origin latency-us expression (e.g. ``COALESCE("ottfb", "ttfb"*1000000.0)``)
- ``{ottlb_p50}`` / ``{ottlb_p95}`` — ``"NULL"`` or a MEDIAN/APPROX_QUANTILE on ``"ottlb"``
- ``{cdn_ovh}`` — ``"NULL"`` or ``MEDIAN("elapsed" - "ottlb") / 1000.0``
- ``{ost_5xx}`` — ``"NULL"`` or origin-5xx error-rate expression
- ``{obytes_p50}`` — ``"NULL"`` or ``MEDIAN("obytes")``
- ``{table}`` — quoted base-table identifier (via ``_safe_table``)
- ``{where}`` — pre-built WHERE clause (uses ``?`` params)
- ``{grouping_clause}`` — ``'GROUP BY GROUPING SETS ((), ("edge"))'`` or ``""``

Output columns per row:
``(edge_group, is_total, requests, total_misses, total_passes,
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
            SELECT "rid", "pop", "ottfb"
            FROM {table}
            WHERE {where} AND "edge" = true AND "ottfb" IS NOT NULL
        ),
        shield_logs AS (
            SELECT "prid", "pop", "ottfb", "ttfb"
            FROM {table}
            WHERE {time_where} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != ''
        )
        SELECT
          e.pop                                                                    AS edge_pop,
          s.pop                                                                    AS shield_pop,
          COUNT(*)                                                                 AS requests,
          PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (e.ottfb - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p50_ms,
          PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (e.ottfb - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p95_ms,
          PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY (e.ottfb - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p99_ms
        FROM edge_logs e
        INNER JOIN shield_logs s ON s.prid = e.rid
        GROUP BY 1, 2
        ORDER BY requests DESC
        LIMIT ?
    """
"""Edge<-shield POP pair latency analysis via self-join on ``rid``/``prid``.

The shield CTE intentionally drops user filters (only time bounds survive)
so an edge filter like ``pop = DEN`` doesn't strip the shield hit at IAD
before the join.

Inputs (trusted-identifier substitutions):
- ``{table}`` — quoted base-table identifier
- ``{where}`` — full WHERE clause (time + filters) applied to the edge CTE
- ``{time_where}`` — time-only WHERE clause applied to the shield CTE

Parameter binding order: ``edge_params + time_params + [limit]``.

Output columns per row:
``(edge_pop, shield_pop, requests, p50_ms, p95_ms, p99_ms)``
"""


# ── Composite endpoint: get_aggregates → CREATE TEMP TABLE + 8 reads ──────────

AGGREGATES_CREATE_TEMP = (
    "CREATE TEMP TABLE {temp_table} AS "
    "SELECT {select_cols}, {lat_us_expr} AS lat_us "
    "FROM {table} WHERE {where_clause}"
)
"""Materialise a per-request TEMP TABLE for the composite origin endpoint.

Computes the latency expression once at materialization time so the six
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


TEMP_SUMMARY_ROLLUP = """
        SELECT
          COUNT(*) FILTER (WHERE "cache" ILIKE 'MISS%')                                    AS total_misses,
          COUNT(*) FILTER (WHERE "cache" ILIKE 'PASS%')                                    AS total_passes,
          MEDIAN({lat_val}) / 1000.0                                                       AS ottfb_p50_ms,
          APPROX_QUANTILE({lat_val}, 0.75) / 1000.0                                        AS ottfb_p75_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                                        AS ottfb_p95_ms,
          APPROX_QUANTILE({lat_val}, 0.99) / 1000.0                                        AS ottfb_p99_ms,
          {ottlb_p50}                                                                       AS ottlb_p50_ms,
          {ottlb_p95}                                                                       AS ottlb_p95_ms,
          {cdn_ovh}                                                                         AS cdn_overhead_p50_ms,
          {ost_5xx}                                                                         AS origin_error_rate,
          {obytes_p50}                                                                      AS obytes_p50
        FROM {temp_table}
        WHERE ({lat_val} IS NOT NULL)
        """
"""Rollup totals against the per-request TEMP TABLE (no GROUPING SETS).

Inputs (trusted-identifier substitutions only):
- ``{lat_val}`` — typically the literal ``"lat_us"`` (the precomputed column)
- ``{ottlb_p50}`` / ``{ottlb_p95}`` / ``{cdn_ovh}`` / ``{ost_5xx}`` /
  ``{obytes_p50}`` — same shape as ``SUMMARY_GROUPING_SETS``: either
  ``"NULL"`` or the matching aggregate expression
- ``{temp_table}`` — TEMP TABLE name

Output columns per row (one row total):
``(total_misses, total_passes, ottfb_p50_ms, ottfb_p75_ms, ottfb_p95_ms,
   ottfb_p99_ms, ottlb_p50_ms, ottlb_p95_ms, cdn_overhead_p50_ms,
   origin_error_rate, obytes_p50)``
"""


TEMP_SUMMARY_BY_EDGE = """
            SELECT "edge",
              COUNT(*)                                                     AS requests,
              MEDIAN({lat_val}) / 1000.0                                   AS p50_ms,
              APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                    AS p95_ms
            FROM {temp_table}
            WHERE ({lat_val} IS NOT NULL)
            GROUP BY "edge"
            """
"""Per-edge breakdown against the TEMP TABLE.

Inputs:
- ``{lat_val}`` — typically the literal ``"lat_us"``
- ``{temp_table}`` — TEMP TABLE name

Output columns per row: ``(edge, requests, p50_ms, p95_ms)``
"""


TEMP_TIMESERIES = """
        SELECT
          time_bucket({interval}, "timestamp")                              AS ts,
          COUNT(*)                                                          AS miss_count,
          {agg_expr} {unit_conv}                                            AS value
          {edge_col}
        FROM {temp_table}
        WHERE ({lat_expr} IS NOT NULL)
        GROUP BY ts {edge_group}
        ORDER BY ts
        """
"""Time-series against the TEMP TABLE.

Inputs (all trusted-identifier substitutions):
- ``{interval}`` — ``INTERVAL '<n>' seconds|minutes`` literal
- ``{agg_expr}`` — pre-built ``MEDIAN``/``APPROX_QUANTILE`` expression
- ``{unit_conv}`` — ``"/ 1000.0"`` or ``"* 1000.0"``
- ``{edge_col}`` — ``', "edge"'`` when splitting, else ``""``
- ``{temp_table}`` — TEMP TABLE name
- ``{lat_expr}`` — latency expression matching ``agg_expr``
- ``{edge_group}`` — ``', "edge"'`` when splitting, else ``""``

Output columns per row: ``(ts, miss_count, value[, edge])``
"""


TEMP_SLOW_URLS = """
        SELECT
          "url",
          COUNT(*)                                                         AS requests,
          MEDIAN(lat_us) / 1000.0                                          AS p50_ms,
          APPROX_QUANTILE(lat_us, 0.95) / 1000.0                           AS p95_ms,
          APPROX_QUANTILE(lat_us, 0.99) / 1000.0                           AS p99_ms
        FROM {temp_table}
        WHERE lat_us IS NOT NULL AND "url" IS NOT NULL
        GROUP BY "url"
        HAVING COUNT(*) >= ?
        ORDER BY p95_ms DESC
        LIMIT ?
        """
"""Slow URLs against the TEMP TABLE (uses the precomputed ``lat_us`` column).

Inputs:
- ``{temp_table}`` — TEMP TABLE name

The two ``?`` placeholders bind, in order: ``min_requests``, ``limit``.

Output columns per row: ``(url, requests, p50_ms, p95_ms, p99_ms)``
"""


TEMP_STATUS_CODES = """
        SELECT
          CASE
            WHEN "ost" BETWEEN 100 AND 599 THEN "ost"
            ELSE -1
          END                                               AS status,
          COUNT(*)                                          AS count,
          COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()          AS pct
        FROM {temp_table}
        WHERE "ost" IS NOT NULL
        GROUP BY 1
        ORDER BY count DESC
        """
"""Status-code distribution against the TEMP TABLE.

Inputs:
- ``{temp_table}`` — TEMP TABLE name

Output columns per row: ``(status, count, pct)``
"""


TEMP_PATH_BREAKDOWN = """
        SELECT
          "edge",
          COUNT(*)                                                          AS requests,
          MEDIAN(lat_us) / 1000.0                                           AS p50_ms,
          APPROX_QUANTILE(lat_us, 0.95) / 1000.0                            AS p95_ms
        FROM {temp_table}
        WHERE lat_us IS NOT NULL
        GROUP BY "edge"
        """
"""Edge-leg breakdown against the TEMP TABLE.

Inputs:
- ``{temp_table}`` — TEMP TABLE name

Output columns per row: ``(edge, requests, p50_ms, p95_ms)``
"""


TEMP_POP_LATENCY = """
        SELECT
          "pop",
          COUNT(*)                                                          AS requests,
          MEDIAN(lat_us) / 1000.0                                           AS p50_ms,
          APPROX_QUANTILE(lat_us, 0.95) / 1000.0                            AS p95_ms
        FROM {temp_table}
        WHERE lat_us IS NOT NULL AND "pop" IS NOT NULL AND "pop" != ''
        GROUP BY "pop"
        ORDER BY p95_ms DESC
        LIMIT ?
        """
"""POP latency against the TEMP TABLE.

Inputs:
- ``{temp_table}`` — TEMP TABLE name

The trailing ``?`` placeholder binds ``limit``.

Output columns per row: ``(pop, requests, p50_ms, p95_ms)``
"""


TEMP_IP_HEALTH = """
        SELECT
          "oip",
          COUNT(*)                                                            AS requests,
          MEDIAN(lat_us) / 1000.0                                             AS p50_ms,
          APPROX_QUANTILE(lat_us, 0.95) / 1000.0                              AS p95_ms,
          ROUND(COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0
            / NULLIF(COUNT(*), 0), 1)                                         AS error_pct
        FROM {temp_table}
        WHERE "oip" IS NOT NULL AND "oip" != '' AND "ost" IS NOT NULL
        GROUP BY "oip"
        HAVING COUNT(*) >= 10
        ORDER BY error_pct DESC
        LIMIT ?
        """
"""Origin IP health against the TEMP TABLE.

Inputs:
- ``{temp_table}`` — TEMP TABLE name

The trailing ``?`` placeholder binds ``limit``.

Output columns per row: ``(oip, requests, p50_ms, p95_ms, error_pct)``
"""


__all__ = [
    "SUMMARY_GROUPING_SETS",
    "TIMESERIES_BUCKETED",
    "SLOW_URLS",
    "STATUS_CODES",
    "PATH_BREAKDOWN",
    "POP_LATENCY",
    "IP_HEALTH",
    "SHIELDING_ANALYSIS",
    "AGGREGATES_CREATE_TEMP",
    "TEMP_SUMMARY_ROLLUP",
    "TEMP_SUMMARY_BY_EDGE",
    "TEMP_TIMESERIES",
    "TEMP_SLOW_URLS",
    "TEMP_STATUS_CODES",
    "TEMP_PATH_BREAKDOWN",
    "TEMP_POP_LATENCY",
    "TEMP_IP_HEALTH",
]
