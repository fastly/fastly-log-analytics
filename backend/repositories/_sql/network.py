"""SQL templates for `backend.repositories.network`.

Phase 5a extraction. Per-template inputs documented inline; non-trusted
values are bound via DuckDB ``?`` parameters, never interpolated.

See ``backend/repositories/_sql/__init__.py`` for the ownership policy.

The network repository builds queries against a per-request temp table
populated by ``QueryRunner.create_filtered_temp_table``. The format
placeholders below are trusted-identifier substitutions (the temp-table
name and column-presence-aware aggregate expressions). User filter
values reach DuckDB through ``runner.execute(sql, params)`` parameter
binding only.
"""

from __future__ import annotations

# ── Heatmap (ASN x bucket) ───────────────────────────────────────────────────

HEATMAP_BY_ASN_BUCKET = """
            WITH precomputed AS (
                SELECT
                    *,
                    EPOCH_MS(
                        CAST((EPOCH_MS(timestamp)::BIGINT // {bucket_ms}) * {bucket_ms} AS BIGINT)
                    )::TIMESTAMP AS bucket
                FROM {table}
                WHERE {where}
                  AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
            ),
            top_cells AS (
                SELECT
                    asn,
                    bucket,
                    COUNT(*) AS reqs,
                    SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS err_count
                FROM precomputed
                WHERE {where}
                  AND asn IS NOT NULL
                GROUP BY 1, 2
                ORDER BY reqs DESC
                LIMIT {row_limit}
            )
            SELECT
                tc.asn,
                tc.bucket,
                APPROX_QUANTILE(
                    CASE WHEN cache LIKE '%HIT%' AND elapsed > 0
                    THEN resp_bytes * 1000000.0 / elapsed END,
                    0.5
                ) AS throughput_bps,
                APPROX_QUANTILE(tcp_rtt, 0.5)          AS rtt_med_us,
                {rtt_min_expr}           AS rtt_baseline_us,
                {congestion_expr}        AS rtt_congestion_us,
                {ploss_expr}             AS avg_ploss,
                {rtt_var_expr}           AS rtt_jitter_us,
                tc.err_count * 100.0 / NULLIF(tc.reqs, 0) AS error_pct,
                tc.reqs
            FROM precomputed AS {table}
            INNER JOIN top_cells tc
                ON {table}.asn = tc.asn
               AND {table}.bucket = tc.bucket
            GROUP BY tc.asn, tc.bucket, tc.reqs, tc.err_count
            ORDER BY tc.reqs DESC
        """
"""ASN x time-bucket heatmap rows for the Network dashboard.

Inputs (all trusted-identifier substitutions):
- ``{bucket_ms}`` — bucket width in milliseconds (int, derived from
  ``bucket_seconds * 1000``).
- ``{rtt_min_expr}`` — pre-built expression (``"MEDIAN(rtt_min)"`` or
  ``"NULL"`` when the column is absent).
- ``{congestion_expr}`` — pre-built expression for congestion
  (``MEDIAN(COALESCE(tcp_rtt, 0) - COALESCE(rtt_min, 0))`` or ``"NULL"``).
- ``{ploss_expr}`` — ``"AVG(ploss)"`` or ``"NULL"``.
- ``{rtt_var_expr}`` — ``"MEDIAN(rtt_var)"`` or ``"NULL"``.
- ``{table}`` — temp-table identifier from ``create_filtered_temp_table``.
- ``{where}`` — base WHERE expression (typically ``"1=1"`` against the
  already-filtered temp table).
- ``{row_limit}`` — int row cap (``top_n * 200``).

Output columns per row:
``(asn, bucket, throughput_bps, rtt_med_us, rtt_baseline_us,
   rtt_congestion_us, avg_ploss, rtt_jitter_us, error_pct, reqs)``
"""


# ── World map (country x city x bucket) ──────────────────────────────────────

MAP_BY_COUNTRY_BUCKET = """
                WITH precomputed AS (
                    SELECT
                        *,
                        {city_col} AS city,
                        {lat_col}  AS lat,
                        {lon_col}  AS lon,
                        {metro_col} AS metro,
                        EPOCH_MS(
                            CAST((EPOCH_MS(timestamp)::BIGINT // {bucket_ms}) * {bucket_ms} AS BIGINT)
                        )::TIMESTAMP AS bucket
                    FROM {table}
                    WHERE {where}
                      AND country IS NOT NULL AND country != ''
                      AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                ),
                top_cells AS (
                    SELECT
                        country,
                        city,
                        lat,
                        lon,
                        metro,
                        bucket,
                        COUNT(*) AS reqs,
                        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS err_count
                    FROM precomputed
                    WHERE {where}
                    GROUP BY 1, 2, 3, 4, 5, 6
                    ORDER BY reqs DESC
                    LIMIT 5000
                )
                SELECT * FROM (
                    SELECT
                        tc.country,
                        tc.city,
                        tc.lat,
                        tc.lon,
                        tc.metro,
                        tc.bucket,
                        APPROX_QUANTILE({table}.tcp_rtt, 0.5) AS rtt_med_us,
                        {ploss_expr}    AS avg_ploss,
                        tc.err_count * 100.0 / NULLIF(tc.reqs, 0) AS error_pct,
                        tc.reqs
                    FROM precomputed AS {table}
                    INNER JOIN top_cells tc
                        ON {table}.country = tc.country
                       AND ({join_city_col})  IS NOT DISTINCT FROM tc.city
                       AND ({join_lat_col})   IS NOT DISTINCT FROM tc.lat
                       AND ({join_lon_col})   IS NOT DISTINCT FROM tc.lon
                       AND ({join_metro_col}) IS NOT DISTINCT FROM tc.metro
                       AND {table}.bucket = tc.bucket
                    WHERE 1=1
                      AND {table}.tcp_rtt IS NOT NULL AND {table}.tcp_rtt > 0
                    GROUP BY tc.country, tc.city, tc.lat, tc.lon, tc.metro, tc.bucket, tc.reqs, tc.err_count
                ) ranked
                ORDER BY bucket, reqs DESC
            """
"""World-map bucket rows: country x city x bucket aggregated by health metrics.

Inputs (all trusted-identifier substitutions):
- ``{city_col}`` — ``"city"`` when present, otherwise ``"''"`` (used in
  the CTE projection where there's no name collision).
- ``{lat_col}`` / ``{lon_col}`` — column name or ``"NULL"`` when absent
  (CTE projection).
- ``{metro_col}`` — column name or ``"NULL"`` when absent (CTE projection).
- ``{join_city_col}`` / ``{join_lat_col}`` / ``{join_lon_col}`` /
  ``{join_metro_col}`` — same logical column but qualified with the
  temp-table name (e.g. ``"t_abc.city"``) when present, NULL/'' literal
  otherwise. Used inside the JOIN ON clause where the bare column name
  is ambiguous because ``top_cells`` aliases the same names on its side.
- ``{bucket_ms}`` — bucket width in milliseconds (int).
- ``{ploss_expr}`` — ``"AVG(ploss)"`` or ``"NULL"``.
- ``{table}`` — temp-table identifier.
- ``{where}`` — base WHERE expression, optionally extended with
  ``" AND asn = ?"`` for the map_asn drill-down (the ``?`` is bound via
  parameters, not interpolated).

The inner query caps to 5000 rows by request volume — past that, dot
density on the map UI is invisible. The outer ``ORDER BY bucket, reqs DESC``
restores the chronological order downstream code expects.

Output columns per row:
``(country, city, lat, lon, metro, bucket, rtt_med_us, avg_ploss,
   error_pct, reqs)``
"""


# ── Metro leaderboard ────────────────────────────────────────────────────────

METRO_LEADERBOARD = """
                WITH top_cells AS (
                    SELECT
                        country,
                        {city_col}   AS city,
                        {region_col} AS region,
                        {metro_col}  AS metro,
                        COUNT(*) AS reqs,
                        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS err_count
                    FROM {table}
                    WHERE {where}
                      AND country IS NOT NULL AND country != ''
                      AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                    GROUP BY 1, 2, 3, 4
                    ORDER BY reqs DESC
                    LIMIT 100
                )
                SELECT
                    tc.country,
                    tc.city,
                    tc.region,
                    tc.metro,
                    APPROX_QUANTILE(tcp_rtt, 0.5) AS rtt_med_us,
                    {ploss_expr} AS avg_ploss,
                    tc.err_count * 100.0 / NULLIF(tc.reqs, 0) AS error_pct,
                    tc.reqs
                FROM {table}
                INNER JOIN top_cells tc
                    ON {table}.country = tc.country
                   AND ({join_city_col})   IS NOT DISTINCT FROM tc.city
                   AND ({join_region_col}) IS NOT DISTINCT FROM tc.region
                   AND ({join_metro_col})  IS NOT DISTINCT FROM tc.metro
                WHERE {where}
                  AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                GROUP BY tc.country, tc.city, tc.region, tc.metro, tc.reqs, tc.err_count
                ORDER BY tc.reqs DESC
            """
"""Top-100 metro/city leaderboard sorted by request volume.

Inputs (all trusted-identifier substitutions):
- ``{city_col}`` — ``"city"`` when present, otherwise ``"''"`` (CTE
  projection).
- ``{region_col}`` — ``"region"`` when present, otherwise ``"''"`` (CTE
  projection).
- ``{metro_col}`` — ``"metro"`` when present, otherwise ``"NULL"`` (CTE
  projection).
- ``{join_city_col}`` / ``{join_region_col}`` / ``{join_metro_col}`` —
  same logical column but qualified with the temp-table name
  (e.g. ``"t_abc.city"``) when present, NULL/'' literal otherwise. Used
  inside the JOIN ON clause where the bare column name is ambiguous
  because ``top_cells`` aliases the same names on its side.
- ``{ploss_expr}`` — ``"AVG(ploss)"`` or ``"NULL"``.
- ``{table}`` — temp-table identifier.
- ``{where}`` — base WHERE expression (typically ``"1=1"``).

Output columns per row:
``(country, city, region, metro, rtt_med_us, avg_ploss, error_pct, reqs)``
"""


# ── Speed distribution by ASN ────────────────────────────────────────────────

SPEED_DISTRIBUTION_BY_ASN = """
                SELECT asn, c_speed, COUNT(*) AS cnt FROM {table}
                WHERE {where} AND asn IN ({placeholders})
                  AND c_speed IS NOT NULL AND c_speed != ''
                GROUP BY asn, c_speed
                ORDER BY asn, cnt DESC
                """
"""Client-speed (c_speed) distribution per top ASN, used to render
the leaderboard's connection-class mix.

Inputs (all trusted-identifier substitutions):
- ``{table}`` — temp-table identifier.
- ``{where}`` — base WHERE expression.
- ``{placeholders}`` — comma-separated ``?`` placeholders matching the
  number of top ASNs being bound (e.g. ``"?,?,?"`` for 3 ASNs). The
  actual ASN integers are passed through ``runner.execute`` as
  parameters, never interpolated.

Output columns per row: ``(asn, c_speed, cnt)``.
"""


# ── P95/P99 RTT per ASN ──────────────────────────────────────────────────────

RTT_PERCENTILES_BY_ASN = """
                SELECT asn,
                    APPROX_QUANTILE(tcp_rtt, 0.95) AS p95_us,
                    APPROX_QUANTILE(tcp_rtt, 0.99) AS p99_us
                FROM {table}
                WHERE {where} AND asn IN ({placeholders})
                  AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                GROUP BY asn
                """
"""P95/P99 TCP-RTT per top ASN — bulk query, one row per ASN.

Inputs (all trusted-identifier substitutions):
- ``{table}`` — temp-table identifier.
- ``{where}`` — base WHERE expression.
- ``{placeholders}`` — comma-separated ``?`` placeholders matching the
  number of top ASNs being bound. The ASN integers are passed through
  parameter binding, not interpolated.

Output columns per row: ``(asn, p95_us, p99_us)``.
"""


# ── Quality bar (run_bar helper) ─────────────────────────────────────────────

QUALITY_BAR_BY_GROUP = """
            SELECT "{group_col}" AS label, APPROX_QUANTILE(tcp_rtt, 0.5) / 1000.0 AS rtt_ms, COUNT(*) AS reqs
            FROM {table}
            WHERE {rtt_filter}{extra_where}
              AND "{group_col}" IS NOT NULL AND CAST("{group_col}" AS VARCHAR) != ''
            GROUP BY "{group_col}"
            ORDER BY reqs DESC
            LIMIT 25
        """
"""Top-25 RTT-bar rows grouped by a single trusted column.

Inputs (all trusted-identifier substitutions):
- ``{group_col}`` — column name to group by (``country``, ``asn``,
  ``region``, ``pop``). Quoted at use sites with embedded double quotes;
  caller must pass an existing column name from the schema.
- ``{table}`` — base table identifier (``_safe_table`` output).
- ``{rtt_filter}`` — pre-built WHERE clause that already includes
  ``tcp_rtt IS NOT NULL AND tcp_rtt > 0``.
- ``{extra_where}`` — optional extra predicate (must start with a
  leading space and ``AND ...``), e.g. ``" AND country = ?"`` for the
  region rollup. May be the empty string.

Output columns per row: ``(label, rtt_ms, reqs)``.
"""


# ── Distinct country list for the quality endpoint ───────────────────────────

QUALITY_COUNTRIES_DISTINCT = """
        SELECT DISTINCT country FROM {table}
        WHERE {where_clause} AND country IS NOT NULL AND country != ''
        ORDER BY country
    """
"""Distinct country codes in the active window for the quality endpoint.

Inputs (all trusted-identifier substitutions):
- ``{table}`` — base table identifier.
- ``{where_clause}`` — pre-built WHERE clause from ``build_where_clause``.

Output column per row: ``(country,)`` (single string per row).
"""


# ── Quality scatter sample ───────────────────────────────────────────────────

QUALITY_SCATTER = """
            SELECT tcp_rtt / 1000.0 AS rtt_ms, ttfb * 1000.0 AS ttfb_ms,
                   COALESCE(cache, 'UNKNOWN') AS cache_state
            FROM {table}
            WHERE {rtt_filter} AND ttfb IS NOT NULL AND ttfb > 0
            USING SAMPLE 2000
        """
"""Sample of 2000 (rtt_ms, ttfb_ms, cache_state) points for the
quality scatter plot.

Inputs (all trusted-identifier substitutions):
- ``{table}`` — base table identifier.
- ``{rtt_filter}`` — pre-built WHERE clause that already includes
  ``tcp_rtt IS NOT NULL AND tcp_rtt > 0``.

Output columns per row: ``(rtt_ms, ttfb_ms, cache_state)``.
"""


__all__ = [
    "HEATMAP_BY_ASN_BUCKET",
    "MAP_BY_COUNTRY_BUCKET",
    "METRO_LEADERBOARD",
    "SPEED_DISTRIBUTION_BY_ASN",
    "RTT_PERCENTILES_BY_ASN",
    "QUALITY_BAR_BY_GROUP",
    "QUALITY_COUNTRIES_DISTINCT",
    "QUALITY_SCATTER",
]
