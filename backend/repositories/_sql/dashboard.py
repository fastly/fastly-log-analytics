"""SQL templates for `backend.repositories.dashboard`.

Phase 5a extraction. See ``pending-docs/sql_ownership_audit.md`` for the
mechanical recipe and ``backend/repositories/_sql/__init__.py`` for the
ownership policy.

Every template here is a Python format string. Placeholders are
trusted-identifier substitutions only (quoted table names, pre-validated
column expressions, pre-built ``WHERE`` clauses). User-supplied values
(filter literals, search text, page bounds) are bound through DuckDB
``?`` parameters at ``runner.execute(sql, params)`` / ``execute_with_retry``
call sites, never interpolated into these templates.
"""

from __future__ import annotations

# ── Virtual-field unnest top-N ────────────────────────────────────────────────

VIRTUAL_FIELD_EXPLODED_TOP_N = """
                WITH split_data AS (
                    SELECT trim(signal) AS signal
                    FROM (
                        SELECT unnest(string_split("{backing_col}", ',')) AS signal
                        FROM {table_name}
                        WHERE "{backing_col}" IS NOT NULL AND "{backing_col}" != '' AND {where_clause}
                    )
                    WHERE trim(signal) != ''
                ),
                total_count AS (SELECT {requests_metric} AS tc FROM split_data),
                top_values AS (
                    SELECT signal AS value, {requests_metric} AS c
                    FROM split_data GROUP BY 1 ORDER BY 2 DESC LIMIT 10
                )
                SELECT tv.value, tv.c, tc.tc FROM top_values tv CROSS JOIN total_count tc
            """
"""Top-N exploded values for a virtual CSV-backed field (e.g. ``waf_sig_ind``).

Inputs (all trusted-identifier substitutions):
- ``{backing_col}`` — name of the CSV-string backing column (e.g. ``waf_sig``)
- ``{table_name}`` — quoted/safe table identifier (live table or temp table)
- ``{where_clause}`` — pre-built filter clause from ``build_where_clause``
  (when the dashboard inlines into a temp table this is ``"1=1"``)
- ``{requests_metric}`` — ``CANONICAL_METRICS["requests"]`` expression, i.e.
  ``COUNT(*)`` — pre-substituted by the caller so this module stays free of
  ``_base`` imports

Output (per row): ``(value: str, count: int, total: int)`` where ``total``
repeats on every row (cross join with ``total_count``).
"""


# ── conn_requests histogram bucket ────────────────────────────────────────────
# NOTE: bucket labels use en-dash (U+2013), matching the historical inline
# SQL byte-for-byte. The frontend matches the bucket strings exactly.

CONN_REQUESTS_BUCKET = (
    "\n"
    "                SELECT\n"
    "                    CASE\n"
    "                        WHEN \"conn_requests\" = 1 THEN '1'\n"
    "                        WHEN \"conn_requests\" BETWEEN 2 AND 5 THEN '2–5'\n"
    "                        WHEN \"conn_requests\" BETWEEN 6 AND 20 THEN '6–20'\n"
    "                        ELSE '21+'\n"
    "                    END AS bucket,\n"
    "                    {requests_metric} AS c\n"
    "                FROM {table_name}\n"
    '                WHERE "conn_requests" IS NOT NULL AND "conn_requests" > 0 AND {where_clause}\n'
    "                GROUP BY 1\n"
    '                ORDER BY MIN("conn_requests")\n'
    "            "
)
"""Bucketed histogram of ``conn_requests`` (connection-reuse counter).

Inputs:
- ``{requests_metric}`` — ``CANONICAL_METRICS["requests"]``, i.e. ``COUNT(*)``
- ``{table_name}`` — quoted/safe table identifier
- ``{where_clause}`` — pre-built filter clause (``"1=1"`` after temp-table
  materialisation)

Output (per row): ``(bucket_label: str, count: int)``. Bucket labels are
``'1'``, ``'2–5'``, ``'6–20'``, ``'21+'`` (en-dashes preserved
byte-for-byte from the historical inline SQL — the frontend matches these
exact strings).
"""


# ── Time series chart ─────────────────────────────────────────────────────────

TIME_SERIES = """
                    SELECT {time_bucket_select},
                           {value_expr} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL{extra_where} AND {where_clause}
                    GROUP BY 1 ORDER BY 1
                """
"""Per-bucket time-series for a chart metric.

Inputs (all trusted-identifier substitutions):
- ``{time_bucket_select}`` — output of ``time_bucket_select(interval)``
- ``{value_expr}`` — pre-built metric expression from ``CANONICAL_METRICS``
  (already formatted with any sub-placeholders like ``{cache_col}``)
- ``{table_name}`` — quoted/safe table identifier
- ``{extra_where}`` — additional ``" AND <expr>"`` (note leading space) to
  inject after ``timestamp IS NOT NULL``, or the empty string. Used for
  percentile-latency charts that need ``AND <elapsed_col> IS NOT NULL``.
- ``{where_clause}`` — pre-built filter clause from ``build_where_clause``

Output (per row): ``(bucket_timestamp, value: float | None)``. A third
``category`` column may appear in future variants — the dashboard tolerates
its absence.
"""


# ── Map data (country aggregate) ──────────────────────────────────────────────

MAP_DATA_BY_COUNTRY = """
                    SELECT "country" AS country, {requests_metric} AS count
                    FROM {table_name}
                    WHERE "country" IS NOT NULL AND {where_clause}
                    GROUP BY 1
                """
"""Per-country request count for the dashboard choropleth.

Inputs:
- ``{requests_metric}`` — ``CANONICAL_METRICS["requests"]`` (``COUNT(*)``)
- ``{table_name}`` — quoted/safe table identifier
- ``{where_clause}`` — pre-built filter clause from ``build_where_clause``

Output (per row): ``(country_code: str, count: int)``.
"""


# ── Field values: bot UA enumeration ──────────────────────────────────────────

FIELD_VALUES_BOT_UA = """
            SELECT ua, {requests_metric} AS cnt
            FROM {table_name}
            WHERE {where_clause} AND ua IS NOT NULL {ua_filter}
            GROUP BY ua
            ORDER BY cnt DESC
            LIMIT 5000
        """
"""Unique UA strings + counts for the virtual ``_bot_name`` field.

The repository pre-matches the result rows against the bot-source matcher
in Python — the SQL stays deliberately broad and the matcher resolves the
bot name.

Inputs:
- ``{requests_metric}`` — ``CANONICAL_METRICS["requests"]`` (``COUNT(*)``)
- ``{table_name}`` — quoted/safe table identifier
- ``{where_clause}`` — pre-built filter clause from ``build_where_clause``
- ``{ua_filter}`` — additional ``AND regexp_matches(ua, '...')`` clause or
  the empty string. Always built from a trusted hard-coded regex pattern
  in ``backend.utils.bot_sources.get_bot_regex_pattern``.

Output (per row): ``(ua: str, cnt: int)``. The hard ``LIMIT 5000`` caps the
cost of the downstream Python-side bot-matching loop.
"""


# ── Field values: virtual CSV-backed signals lookup ───────────────────────────

FIELD_VALUES_VIRTUAL_SIGNALS = """
            SELECT trim(signal) AS value, {requests_metric} AS count
            FROM (
                SELECT unnest(string_split("{backing_col}", ',')) AS signal
                FROM {table_name}
                WHERE {where_clause} AND "{backing_col}" IS NOT NULL AND "{backing_col}" != ''
            )
            WHERE trim(signal) != '' {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """
"""Field-values picker for CSV-backed virtual fields (waf_sig_ind, etc.).

Used by the dashboard filter picker so click-to-filter on a specific signal
routes through the same unnest path as the top-N aggregation.

Inputs:
- ``{requests_metric}`` — ``CANONICAL_METRICS["requests"]`` (``COUNT(*)``)
- ``{backing_col}`` — backing CSV column name (e.g. ``waf_sig``);
  pre-sanitised by the repository to ``[A-Za-z0-9_]+``
- ``{table_name}`` — quoted/safe table identifier
- ``{where_clause}`` — pre-built filter clause from ``build_where_clause``
  (with the field's own filter excluded so the picker shows all values)
- ``{search_cond}`` — optional ``AND trim(signal) ILIKE ?`` clause or empty.
  The ``?`` is bound through ``params`` — never interpolated.
- ``{limit}`` — integer page limit, validated upstream

Output (per row): ``(value: str, count: int)``.
"""


# ── Field values: native column lookup ────────────────────────────────────────

FIELD_VALUES_NATIVE_COLUMN = """
            SELECT "{clean_field}" AS value, {requests_metric} AS count
            FROM {table_name}
            WHERE {where_clause} {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """
"""Field-values picker for native (non-virtual) columns.

Inputs:
- ``{clean_field}`` — column name, pre-sanitised to ``[A-Za-z0-9_]+``
- ``{requests_metric}`` — ``CANONICAL_METRICS["requests"]`` (``COUNT(*)``)
- ``{table_name}`` — quoted/safe table identifier
- ``{where_clause}`` — pre-built filter clause (caller excludes the field's
  own filter so the picker shows all available values)
- ``{search_cond}`` — optional ``AND CAST(...) ILIKE ?`` clause (with extra
  ``IN (...)`` placeholders for country / asn). Bound through ``params``.
- ``{limit}`` — integer page limit

Output (per row): ``(value, count: int)``. Value type matches the column.
"""


__all__ = [
    "VIRTUAL_FIELD_EXPLODED_TOP_N",
    "CONN_REQUESTS_BUCKET",
    "TIME_SERIES",
    "MAP_DATA_BY_COUNTRY",
    "FIELD_VALUES_BOT_UA",
    "FIELD_VALUES_VIRTUAL_SIGNALS",
    "FIELD_VALUES_NATIVE_COLUMN",
]
