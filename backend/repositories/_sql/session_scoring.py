"""SQL builders for `backend.repositories.session_scoring` analytics reads.

``src`` is a FROM-clause source built by ``scoring_source`` (a ``_safe_table_name`` identifier or a
typed-NULL-padded subquery over it); ``ts_pred`` is the time predicate built by the router's
``_scoring_time_window`` (clamped to the analyst invite window). Neither is raw request input.
Integer substitutions (``limit``, ``threshold``) are coerced with ``int()`` at the call site.
"""

from __future__ import annotations

TOP_FLAGGED = """
        SELECT
            timestamp,
            edge_sid,
            edge_score,
            edge_score_l1,
            edge_score_l2,
            edge_cookie_compliance,
            edge_score_reason,
            ip,
            ua,
            url,
            status,
            country
        FROM {src}
        WHERE edge_score IS NOT NULL
          AND {ts_pred}
        ORDER BY edge_score DESC, timestamp DESC
        LIMIT {limit}
    """

SCORE_DISTRIBUTION = """
        SELECT
            date_trunc('hour', timestamp) AS hour,
            CASE
                WHEN edge_score < 25 THEN '0-25'
                WHEN edge_score < 50 THEN '25-50'
                WHEN edge_score < 75 THEN '50-75'
                ELSE '75-100'
            END AS bucket,
            COUNT(*) AS count
        FROM {src}
        WHERE edge_score IS NOT NULL
          AND {ts_pred}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

COMPLIANCE_BREAKDOWN = """
        SELECT
            date_trunc('hour', timestamp) AS hour,
            edge_cookie_compliance AS compliance,
            COUNT(*) AS count
        FROM {src}
        WHERE edge_cookie_compliance IS NOT NULL
          AND edge_cookie_compliance != ''
          AND {ts_pred}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

LATENCY_TIMESERIES = """
        SELECT
            {select_sql}
        FROM {src}
        -- Scored rows are edge requests even when the scoring restart left
        -- edge != true on them; include them so the distribution isn't empty.
        WHERE (edge = true OR edge_score IS NOT NULL)
          AND {ts_pred}
        GROUP BY 1
        ORDER BY 1
    """


def latency_timeseries_sql(src: str, ts_pred: str, bucket: str, has_rtt: bool, has_exec: bool) -> str:
    """Per-bucket scored / fail-open counts plus optional RTT and Wasm-exec percentiles.

    ``bucket`` is the whitelisted literal ``"minute"`` or ``"hour"`` (never user input).
    """
    select_parts = [
        f"date_trunc('{bucket}', timestamp) AS hour",
        "COUNT(*) FILTER (WHERE edge_score IS NOT NULL) AS scored_count",
        "COUNT(*) FILTER (WHERE edge_score_reason ILIKE '%compute-unavailable%') AS fail_open_count",
        "COUNT(*) AS total_count",
    ]
    if has_rtt:
        select_parts += [
            "CAST(quantile_cont(edge_score_rtt_us, 0.5)  FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p50_us",
            "CAST(quantile_cont(edge_score_rtt_us, 0.95) FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p95_us",
            "CAST(quantile_cont(edge_score_rtt_us, 0.99) FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p99_us",
        ]
    if has_exec:
        select_parts += [
            "CAST(quantile_cont(edge_score_exec_us, 0.5)  FILTER (WHERE edge_score_exec_us IS NOT NULL) AS BIGINT) AS exec_p50_us",
            "CAST(quantile_cont(edge_score_exec_us, 0.95) FILTER (WHERE edge_score_exec_us IS NOT NULL) AS BIGINT) AS exec_p95_us",
        ]
    select_sql = ",\n            ".join(select_parts)
    return LATENCY_TIMESERIES.format(select_sql=select_sql, src=src, ts_pred=ts_pred)


# Health snapshot. Literal ``{{`` / ``}}`` below are DuckDB struct braces escaped for str.format.
HEALTH_SNAPSHOT = """
        WITH edge_rows AS (
            SELECT
                edge_score,
                edge_score_l2,
                edge_score_reason,
                edge_cookie_compliance,
                edge_sid{rtt_cte}{exec_cte}
            FROM {src}
            -- Scored rows ARE edge requests (the scorer only runs at the edge),
            -- but the scoring restart means the capture VCL may not stamp
            -- edge=true on them. Include them explicitly so scored traffic isn't
            -- dropped from the health view (Fire Rate 0% despite live scoring).
            WHERE (edge = true OR edge_score IS NOT NULL)
              AND {ts_pred}
        ),
        scored AS (
            SELECT * FROM edge_rows WHERE edge_score IS NOT NULL
        ),
        reason_rows AS (
            SELECT trim(reason) AS reason
            FROM (
                SELECT unnest(string_split(edge_score_reason, ',')) AS reason
                FROM scored
                WHERE edge_score_reason IS NOT NULL AND edge_score_reason != ''
            )
            WHERE trim(reason) != ''
        ),
        top_reasons AS (
            SELECT reason, COUNT(*) AS n
            FROM reason_rows
            GROUP BY reason
            ORDER BY n DESC
            LIMIT 10
        ),
        fail_open_reasons AS (
            -- Fail-open breakdown by EXACT reason string so the operator can
            -- tell compute-unavailable-503 (scorer timeout/unavailable) from
            -- -500 (Wasm trap) / -401 (auth mismatch) / internal-error-keys
            -- (KV/key load) — each is a distinct remediation. The VCL deliver
            -- snippet rewrites every non-200 scorer response to
            -- "compute-unavailable-<status>", so the scorer's 401-unauthorized
            -- lands here as "compute-unavailable-401" (caught by the first
            -- filter) — there is no bare "unauthorized" log reason (EC-05). The
            -- Rust scorer returns 200 + a bare "internal-error-keys" reason on a
            -- key-load failure (caught by the second filter). NOT limited (unlike
            -- top_reasons) — fail-opens are rare and every class matters.
            SELECT reason, COUNT(*) AS n
            FROM reason_rows
            WHERE reason ILIKE 'compute-unavailable%'
               OR reason ILIKE 'internal-error%'
            GROUP BY reason
            ORDER BY n DESC
        )
        SELECT
            (SELECT COUNT(*) FROM edge_rows)                                    AS total_edge_rows,
            (SELECT COUNT(*) FROM scored)                                       AS scored_rows,
            (SELECT COUNT(DISTINCT edge_sid) FROM scored WHERE edge_sid <> '') AS distinct_sids,
            (SELECT AVG(edge_score) FROM scored)                                AS avg_score,
            (SELECT quantile_cont(edge_score, 0.5) FROM scored)                 AS p50_score,
            (SELECT quantile_cont(edge_score, 0.95) FROM scored)                AS p95_score,
            (SELECT MAX(edge_score) FROM scored)                                AS max_score,
            (SELECT COUNT(*) FROM scored
              WHERE edge_score_reason ILIKE '%compute-unavailable%')            AS scorer_errors,
            (SELECT list({{'reason': reason, 'count': n}}) FROM top_reasons)    AS top_reasons,
            (SELECT list({{'reason': reason, 'count': n}}) FROM fail_open_reasons) AS fail_open_breakdown,
            -- Matrix-staleness signal: fraction of scored rows whose L2
            -- transition score is "high" (≥50), meaning the matrix gave
            -- that transition low probability. When this fraction climbs
            -- (3× baseline per research §7.5), the matrix is drifting
            -- relative to current traffic and a retrain is warranted.
            -- L2 of 0 means "didn't trip a rare-transition rule" — used
            -- in the denominator of the staleness fraction filtered to
            -- rows we ACTUALLY evaluated L2 on (excludes cookie-missing
            -- shortcuts that bypass the matrix).
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 IS NOT NULL)       AS l2_evaluated,
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 >= 50)             AS l2_high_count{latency_select}
    """


def health_snapshot_sql(src: str, ts_pred: str, has_rtt: bool, has_exec: bool) -> str:
    """One-row scoring health aggregate; latency columns are optional (older services lack them)."""
    rtt_cte = ",\n                edge_score_rtt_us" if has_rtt else ""
    exec_cte = ",\n                edge_score_exec_us" if has_exec else ""
    lat_parts: list[str] = []
    if has_rtt:
        # RTT over ALL routed edge rows (incl. fail-opens, which sit ≈the
        # timeout budget) — that's the distribution to compare against the
        # backend timeout when tuning the fail-open rate.
        lat_parts += [
            "(SELECT quantile_cont(edge_score_rtt_us, 0.5)  FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p50_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.95) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p95_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.99) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p99_us",
            "(SELECT MAX(edge_score_rtt_us) FROM edge_rows)                                                     AS rtt_max_us",
        ]
    if has_exec:
        # Exec over SCORED rows only (fail-opens never carry a self-time).
        lat_parts += [
            "(SELECT quantile_cont(edge_score_exec_us, 0.5)  FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p50_us",
            "(SELECT quantile_cont(edge_score_exec_us, 0.95) FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p95_us",
        ]
    latency_select = (",\n            " + ",\n            ".join(lat_parts)) if lat_parts else ""
    return HEALTH_SNAPSHOT.format(
        rtt_cte=rtt_cte, exec_cte=exec_cte, src=src, ts_pred=ts_pred, latency_select=latency_select
    )


# Threshold preview: one CTE pass over per-sid max scores, optionally LEFT JOINed to the label VALUES list.
THRESHOLD_PREVIEW_LABELED = """
                WITH sid_scores AS (
                    SELECT edge_sid, MAX(edge_score) AS max_score
                    FROM {src}
                    -- edge_score IS NOT NULL already scopes to scored (edge)
                    -- requests; an explicit edge = true would drop scored rows
                    -- whose edge flag the scoring restart left unset.
                    WHERE edge_score IS NOT NULL
                      AND edge_sid IS NOT NULL
                      AND edge_sid <> ''
                      AND {ts_pred}
                    GROUP BY edge_sid
                ),
                labels(edge_sid, label) AS (
                    VALUES {values_placeholders}
                ),
                joined AS (
                    SELECT s.max_score, l.label
                    FROM sid_scores s
                    LEFT JOIN labels l USING (edge_sid)
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN max_score >= {threshold} THEN 1 ELSE 0 END) AS flagged_total,
                    SUM(CASE WHEN max_score >= {threshold} AND label = 'good' THEN 1 ELSE 0 END) AS flagged_good,
                    SUM(CASE WHEN max_score >= {threshold} AND label = 'bad'  THEN 1 ELSE 0 END) AS flagged_bad,
                    SUM(CASE WHEN max_score <  {threshold} AND label = 'good' THEN 1 ELSE 0 END) AS passed_good,
                    SUM(CASE WHEN max_score <  {threshold} AND label = 'bad'  THEN 1 ELSE 0 END) AS passed_bad
                FROM joined
            """

THRESHOLD_PREVIEW_UNLABELED = """
                WITH sid_scores AS (
                    SELECT edge_sid, MAX(edge_score) AS max_score
                    FROM {src}
                    -- edge_score IS NOT NULL already scopes to scored (edge)
                    -- requests; an explicit edge = true would drop scored rows
                    -- whose edge flag the scoring restart left unset.
                    WHERE edge_score IS NOT NULL
                      AND edge_sid IS NOT NULL
                      AND edge_sid <> ''
                      AND {ts_pred}
                    GROUP BY edge_sid
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN max_score >= {threshold} THEN 1 ELSE 0 END) AS flagged_total
                FROM sid_scores
            """
