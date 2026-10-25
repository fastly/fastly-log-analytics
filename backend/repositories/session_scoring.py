"""Repository for session-scoring analytics queries.

Owns the DuckDB read path that the ``backend.routers.session_scoring``
admin endpoints depend on. The router constructs the per-endpoint SQL
(table-name validated via ``_safe_table_name``) and delegates execution
+ telemetry attribution to :func:`query_logs` here. Per-sid event
hydration for ROC-AUC evaluation lives in :func:`fetch_session_events`
and :func:`reconstruct_labeled_sessions`.

Why the per-call connection open/close: ``get_connection()`` opens a
fresh DuckDB connection by design — independent connections beat
shared-cursor serialization under load (see backend/core/duckdb.py).
Holding them open here was the root cause of the 2026-06-01
admin-polling RAM blow-up.
"""

from __future__ import annotations

import logging
import sys as _sys
import time as _time
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger(__name__)

# ── SQL templates (folded from backend/repositories/_sql/session_scoring.py) ──

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

sql_scoring: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.session_scoring", _sys.modules[__name__])


def query_logs(service_id: str, sql: str, params: tuple = ()) -> list[dict]:
    """Execute ``sql`` against the per-service logs view and return
    ``list[dict]``.

    ``params`` is passed through to ``con.execute`` so callers can use
    parametrized queries (e.g. ``WHERE edge_sid IN (?, ?, ?)``) without
    string-formatting user-controlled values into the SQL.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.core.iceberg import execute_with_stale_view_retry
    from backend.repositories._base import _compact_sql_for_debug
    from backend.utils.telemetry import get_queries

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    con = None
    t0 = _time.monotonic()
    try:
        skip_view = not svcconfig.is_durable_serving_mode(src)
        con = get_connection(source=src, max_wait=3, skip_view_update=skip_view, read_only=True)

        def _exec(c: Any) -> Any:
            return c.execute(sql, params).fetchall() if params else c.execute(sql).fetchall()

        rows = execute_with_stale_view_retry(con, src, _exec)
        cols = [d[0] for d in con.description] if con.description else []
        result = [dict(zip(cols, r)) for r in rows]
        get_queries().append(
            {
                "sql": _compact_sql_for_debug(sql.strip()),
                "time_ms": round((_time.monotonic() - t0) * 1000, 2),
                "rows": len(result),
                "engine": "DuckDB",
            }
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                logger.debug("query_logs: con.close failed (ignored)", exc_info=True)


# Core edge-scoring columns emitted by the scoring VCL. Absent on services
# where scoring has never been provisioned, so scoring reads must be able to
# bind them even when the parquet schema doesn't carry them yet — otherwise
# DuckDB raises a binder error that ``query_logs`` turns into an HTTP 400.
# Canonical home (the router's ``_scoring_source`` aliases this) because the
# router imports this repository, not the other way round. The optional
# latency cols (edge_score_rtt_us/exec_us) are handled separately by the
# router's has_rtt/has_exec gating and are intentionally NOT synthesized here.
_SCORING_COLUMN_TYPES = {
    "edge_score": "DOUBLE",
    "edge_score_l1": "DOUBLE",
    "edge_score_l2": "DOUBLE",
    "edge_sid": "VARCHAR",
    "edge_score_reason": "VARCHAR",
    "edge_cookie_compliance": "VARCHAR",
}


def scoring_source(table: str, cols: set[str]) -> str:
    """A FROM-clause source for ``table`` guaranteed to expose the core
    scoring columns, synthesizing any missing ones as typed NULLs.

    Returns ``table`` unchanged when all core columns are present — or when
    ``cols`` is empty (schema unknown / view not ready), so we never risk a
    duplicate-column error by adding a column that actually exists. Otherwise
    wraps the table in ``(SELECT *, CAST(NULL AS T) AS missing_col, ... FROM
    table)``; ``table`` is a bare identifier (``logs_<svc>`` via
    ``_safe_table_name``) so the subquery is valid, and projection push-down
    keeps the inner ``SELECT *`` from actually reading every column.
    """
    if not cols:
        return table
    missing = [(c, t) for c, t in _SCORING_COLUMN_TYPES.items() if c not in cols]
    if not missing:
        return table
    synth = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in missing)
    return f"(SELECT *, {synth} FROM {table})"


def _scoring_table_columns(service_id: str, table: str) -> set[str]:
    """Column-name set for ``table``, or empty set on error (view not ready /
    table missing). Used to gate :func:`scoring_source`."""
    try:
        rows = describe_table(service_id, table)
    except HTTPException:
        return set()
    return {str(r.get("column_name")) for r in rows if r.get("column_name")}


def fetch_session_events(
    service_id: str,
    sids: list[str],
    since_days: int = 30,
    limit_per_sid: int = 500,
    *,
    ts_predicate: str | None = None,
) -> dict[str, list[dict]]:
    """Return ``{sid: [{ts, url, status, ip, ua, edge_score, edge_cookie_compliance, edge_score_reason}, ...]}``
    for every sid in ``sids`` whose events landed in DuckDB within the
    last ``since_days`` days.

    ``ts_predicate`` (H3): when set, this SQL timestamp predicate replaces the
    relative ``now() - INTERVAL since_days DAY`` filter — the analyst-reachable
    events route passes a predicate clamped to the invite window so an analyst
    can't read events outside their grant. Internal/admin callers leave it None
    and keep the relative window.

    Sids with no rows in the window are dropped from the result. The
    per-sid event cap is a safety bound — a runaway session with 10k+
    requests would otherwise bloat the response; 500 covers any
    realistic browsing pattern.
    """
    if not sids:
        return {}

    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    # On a service where scoring was never provisioned the log table lacks the
    # edge_sid / edge_score* / edge_cookie_compliance columns this query binds.
    # Wrap it so they resolve to NULL (→ empty result) instead of a binder
    # error → HTTP 400. This route (/scoring/sessions/{sid}/events) is reachable
    # pre-enablement (no label gate), unlike the AUC/curve callers.
    src = scoring_source(table, _scoring_table_columns(service_id, table))
    placeholders = ",".join("?" for _ in sids)
    where_ts = ts_predicate or f"timestamp >= now() - INTERVAL {int(since_days)} DAY"
    # Push the per-sid LIMIT into SQL via row_number() OVER (PARTITION BY
    # edge_sid ORDER BY timestamp). The previous shape let DuckDB
    # materialise the full result set in Python before the len-check
    # ran — a single attacker session with millions of events could OOM
    # the backend before any Python code saw a row.
    per_sid_cap = int(limit_per_sid)
    sql = f"""
        WITH ranked AS (
            SELECT edge_sid, timestamp AS ts, url, status, ip, ua,
                   edge_score, edge_cookie_compliance, edge_score_reason,
                   row_number() OVER (PARTITION BY edge_sid ORDER BY timestamp) AS _rn
            FROM {src}
            WHERE edge_sid IN ({placeholders})
              AND {where_ts}
        )
        SELECT edge_sid, ts, url, status, ip, ua,
               edge_score, edge_cookie_compliance, edge_score_reason
        FROM ranked
        WHERE _rn <= {per_sid_cap}
        ORDER BY edge_sid, ts
    """
    rows = query_logs(service_id, sql, tuple(sids))

    grouped: dict[str, list[dict]] = {}
    for r in rows:
        sid = r.get("edge_sid")
        if not sid:
            continue
        bucket = grouped.setdefault(sid, [])
        if len(bucket) >= limit_per_sid:
            continue
        ts = r.get("ts")
        if ts is None:
            ts_str: str | None = None
        elif hasattr(ts, "isoformat"):
            ts_str = ts.isoformat()
        else:
            ts_str = str(ts)
        bucket.append(
            {
                "ts": ts_str,
                "url": r.get("url") or "/",
                "status": r.get("status"),
                "ip": r.get("ip"),
                "ua": r.get("ua"),
                "edge_score": r.get("edge_score"),
                "edge_cookie_compliance": r.get("edge_cookie_compliance"),
                "edge_score_reason": r.get("edge_score_reason"),
            }
        )
    return grouped


def reconstruct_labeled_sessions(
    service_id: str,
    labels: list[dict],
    *,
    ts_predicate: str | None = None,
) -> list[tuple[dict, str]]:
    """Replay each labeled sid into the ``{session_id, events:[{ts,url}]}``
    shape that ``evaluate()`` expects.

    Returns ``(session_dict, label)`` tuples ready to pass to ``evaluate``.
    Sids that don't appear in DuckDB (haven't been ingested yet, or were
    rotated away) are dropped silently — they contribute nothing to AUC
    either way.
    """
    if not labels:
        return []
    sid_to_label = {row["sid"]: row["label"] for row in labels if row.get("sid")}
    if not sid_to_label:
        return []
    grouped = fetch_session_events(service_id, list(sid_to_label.keys()), since_days=30, ts_predicate=ts_predicate)
    out: list[tuple[dict, str]] = []
    for sid, label in sid_to_label.items():
        events = grouped.get(sid, [])
        if not events:
            continue
        # max_edge_score is what evaluate_from_persisted_scores consumes:
        # taking MAX across the session matches the production VCL
        # behavior — a session is operationally caught at its worst
        # single transition, not its average. None-valued rows are
        # excluded so a sid with only un-scored events doesn't collapse
        # to max_edge_score=0.
        # Filter+cast in one pass: ``e.get("edge_score")`` narrows to non-None
        # after the comprehension's `is not None` guard, but mypy doesn't
        # carry that through, so we re-bind via a typed walrus.
        scored_values: list[float] = []
        for e in events:
            v = e.get("edge_score")
            if v is not None:
                scored_values.append(v)
        max_score = max(scored_values) if scored_values else None
        out.append(
            (
                {
                    "session_id": sid,
                    "events": events,
                    "max_edge_score": max_score,
                },
                label,
            )
        )
    return out


# ── Analytics reads (SQL lives in ``_sql/session_scoring.py``) ────────────────
# ``src`` / ``ts_pred`` come from the router's ``scoring_source`` / ``_scoring_time_window``
# helpers; every function goes through ``query_logs`` so telemetry and the 404/400 mapping apply.


def top_flagged_rows(service_id: str, src: str, ts_pred: str, limit: int) -> list[dict]:
    """Highest-scored recent rows (score DESC, newest first), at most ``limit``."""
    return query_logs(service_id, sql_scoring.TOP_FLAGGED.format(src=src, ts_pred=ts_pred, limit=int(limit)))


def score_distribution_rows(service_id: str, src: str, ts_pred: str) -> list[dict]:
    """Hourly counts per score bucket (0-25 / 25-50 / 50-75 / 75-100)."""
    return query_logs(service_id, sql_scoring.SCORE_DISTRIBUTION.format(src=src, ts_pred=ts_pred))


def compliance_breakdown_rows(service_id: str, src: str, ts_pred: str) -> list[dict]:
    """Hourly counts grouped by ``edge_cookie_compliance``."""
    return query_logs(service_id, sql_scoring.COMPLIANCE_BREAKDOWN.format(src=src, ts_pred=ts_pred))


def latency_timeseries_rows(
    service_id: str, src: str, ts_pred: str, bucket: str, has_rtt: bool, has_exec: bool
) -> list[dict]:
    """Per-bucket scored / fail-open counts and (when the columns exist) RTT and Wasm-exec percentiles."""
    return query_logs(service_id, sql_scoring.latency_timeseries_sql(src, ts_pred, bucket, has_rtt, has_exec))


def health_snapshot_row(service_id: str, src: str, ts_pred: str, has_rtt: bool, has_exec: bool) -> dict:
    """The single aggregate row behind the scoring-health card (``{}`` when the query returns nothing)."""
    rows = query_logs(service_id, sql_scoring.health_snapshot_sql(src, ts_pred, has_rtt, has_exec))
    return rows[0] if rows else {}


def threshold_preview_counts(
    service_id: str, src: str, ts_pred: str, threshold: int, labeled_pairs: list[tuple[str, str]]
) -> dict:
    """Per-sid max-score bucket counts at ``threshold``, joined to ``(sid, label)`` pairs when any exist.

    Without labels only ``total`` / ``flagged_total`` are returned (single aggregate scan, no join).
    """
    threshold_int = int(threshold)
    if labeled_pairs:
        values_placeholders = ", ".join("(?, ?)" for _ in labeled_pairs)
        values_params: tuple = tuple(p for pair in labeled_pairs for p in pair)
        single_sql = sql_scoring.THRESHOLD_PREVIEW_LABELED.format(
            src=src, ts_pred=ts_pred, values_placeholders=values_placeholders, threshold=threshold_int
        )
        rows = query_logs(service_id, single_sql, values_params)
    else:
        agg_sql = sql_scoring.THRESHOLD_PREVIEW_UNLABELED.format(src=src, ts_pred=ts_pred, threshold=threshold_int)
        rows = query_logs(service_id, agg_sql)
    return rows[0] if rows else {}


def describe_table(service_id: str, table: str) -> list[dict]:
    """``DESCRIBE`` rows (``column_name``, ``column_type``, ...) for a ``_safe_table_name`` identifier."""
    return query_logs(service_id, f"DESCRIBE {table}")
