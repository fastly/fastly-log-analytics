"""SQL templates for `backend.repositories.cmcd`.

Per-template inputs are trusted-identifier or pre-built expression
substitutions only. See ``backend/repositories/_sql/__init__.py`` for the
ownership policy.
"""

from __future__ import annotations

OVERVIEW = """
                WITH base_metrics AS (
                    SELECT
                        COUNT(DISTINCT cmcd_sid) AS active_sessions,
                        {rebuffer_rate_expr},
                        {avg_bitrate_expr} AS avg_bitrate,
                        {avg_buffer_length_expr} AS avg_buffer_length,
                        {median_throughput_expr} AS median_throughput
                    FROM {table} WHERE {where}
                ),
                peak_metrics AS (
                    SELECT MAX(cnt) AS peak_viewers FROM (
                        SELECT COUNT(DISTINCT cmcd_sid) AS cnt
                        FROM {table} WHERE {where}
                        GROUP BY {bucket_expr}
                    )
                ),
                dur_metrics AS (
                    SELECT ROUND(AVG(dur), 0) AS avg_dur FROM (
                        SELECT EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
                        FROM {table} WHERE {where}
                        GROUP BY cmcd_sid
                        HAVING dur > 0
                    )
                )
                SELECT
                    b.active_sessions,
                    b.rebuffer_rate,
                    b.avg_bitrate,
                    b.avg_buffer_length,
                    b.median_throughput,
                    p.peak_viewers,
                    d.avg_dur
                FROM base_metrics b
                CROSS JOIN peak_metrics p
                CROSS JOIN dur_metrics d
            """

TIMESERIES_COMBINED = "SELECT {select_cols} FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1"

NEW_SESSIONS = """
                        SELECT {first_bucket} AS bucket, COUNT(*) AS new_sessions
                        FROM (
                            SELECT cmcd_sid, MIN(timestamp) AS first_ts
                            FROM {table} WHERE {where} GROUP BY cmcd_sid
                        ) sub
                        GROUP BY 1
                    """

TOP_CONTENT = """
                WITH per_session AS (
                    SELECT cmcd_cid, cmcd_sid,
                        {rebuffer_agg} AS had_rebuffer,
                        {br_agg} AS avg_br,
                        {bl_agg} AS avg_bl
                    FROM {table}
                    WHERE {where} AND cmcd_cid IS NOT NULL AND cmcd_cid != ''
                    GROUP BY 1, 2
                )
                SELECT
                    cmcd_cid,
                    COUNT(*) AS session_count,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    AVG(avg_br) AS avg_br,
                    AVG(avg_bl) AS avg_bl
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """

REBUFFER_BY_COUNTRY = """
                WITH per_session AS (
                    SELECT country, cmcd_sid,
                        MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer,
                        {mtp_agg} AS median_mtp
                    FROM {table}
                    WHERE {where} AND country IS NOT NULL
                    GROUP BY 1, 2
                )
                SELECT
                    country,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    COUNT(*) AS session_count,
                    AVG(median_mtp) AS median_mtp
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """

REBUFFER_BY_ASN = """
                WITH per_session AS (
                    SELECT asn, cmcd_sid,
                        MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer,
                        {br_agg} AS avg_br,
                        {bl_agg} AS avg_bl
                    FROM {table}
                    WHERE {where} AND asn IS NOT NULL
                    GROUP BY 1, 2
                )
                SELECT
                    asn,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    COUNT(*) AS session_count,
                    AVG(avg_br) AS avg_br,
                    AVG(avg_bl) AS avg_bl
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """

DIST_BOTH = """
                    SELECT 'ot' AS kind, cmcd_ot AS val, COUNT(*) AS cnt
                    FROM {table} WHERE {where} AND cmcd_ot IS NOT NULL AND cmcd_ot != ''
                    GROUP BY 2
                    UNION ALL
                    SELECT 'sf' AS kind, cmcd_sf AS val, COUNT(DISTINCT cmcd_sid) AS cnt
                    FROM {table} WHERE {where} AND cmcd_sf IS NOT NULL AND cmcd_sf != ''
                    GROUP BY 2
                    ORDER BY kind ASC, cnt DESC
                """

DIST_OT = """
                    SELECT cmcd_ot, COUNT(*) AS request_count
                    FROM {table} WHERE {where} AND cmcd_ot IS NOT NULL AND cmcd_ot != ''
                    GROUP BY 1 ORDER BY 2 DESC
                """

DIST_SF = """
                    SELECT cmcd_sf, COUNT(DISTINCT cmcd_sid) AS session_count
                    FROM {table} WHERE {where} AND cmcd_sf IS NOT NULL AND cmcd_sf != ''
                    GROUP BY 1 ORDER BY 2 DESC
                """

SESSION_DURATION_DIST = """
                WITH session_spans AS (
                    SELECT cmcd_sid,
                        EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
                    FROM {table} WHERE {where}
                    GROUP BY cmcd_sid
                )
                SELECT
                    CASE
                        WHEN dur < 30 THEN '<30s'
                        WHEN dur < 120 THEN '30s–2m'
                        WHEN dur < 300 THEN '2–5m'
                        WHEN dur < 600 THEN '5–10m'
                        WHEN dur < 1800 THEN '10–30m'
                        WHEN dur < 3600 THEN '30m–1h'
                        ELSE '>1h'
                    END AS duration_bucket,
                    CASE
                        WHEN dur < 30 THEN 1
                        WHEN dur < 120 THEN 2
                        WHEN dur < 300 THEN 3
                        WHEN dur < 600 THEN 4
                        WHEN dur < 1800 THEN 5
                        WHEN dur < 3600 THEN 6
                        ELSE 7
                    END AS sort_order,
                    COUNT(*) AS session_count
                FROM session_spans
                GROUP BY 1, 2
                ORDER BY 2
            """
