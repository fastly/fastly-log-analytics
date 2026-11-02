"""Session-scoring analytics repository (SQL templates and query helpers)."""

from __future__ import annotations

import logging
import sys as _sys
import time
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger(__name__)

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
        WHERE (edge = true OR edge_score IS NOT NULL)
          AND {ts_pred}
        GROUP BY 1
        ORDER BY 1
    """


def latency_timeseries_sql(src: str, ts_pred: str, bucket: str, has_rtt: bool, has_exec: bool) -> str:
    """Per-bucket scored / fail-open counts plus optional RTT and Wasm-exec percentiles."""
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


HEALTH_SNAPSHOT = """
        WITH edge_rows AS (
            SELECT
                edge_score,
                edge_score_l2,
                edge_score_reason,
                edge_cookie_compliance,
                edge_sid{rtt_cte}{exec_cte}
            FROM {src}
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
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 IS NOT NULL)       AS l2_evaluated,
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 >= 50)             AS l2_high_count{latency_select}
    """


def health_snapshot_sql(src: str, ts_pred: str, has_rtt: bool, has_exec: bool) -> str:
    """One-row scoring health aggregate; latency columns are optional (older services lack them)."""
    rtt_cte = ",\n                edge_score_rtt_us" if has_rtt else ""
    exec_cte = ",\n                edge_score_exec_us" if has_exec else ""
    lat_parts: list[str] = []
    if has_rtt:
        lat_parts += [
            "(SELECT quantile_cont(edge_score_rtt_us, 0.5)  FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p50_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.95) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p95_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.99) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p99_us",
            "(SELECT MAX(edge_score_rtt_us) FROM edge_rows)                                                     AS rtt_max_us",
        ]
    if has_exec:
        lat_parts += [
            "(SELECT quantile_cont(edge_score_exec_us, 0.5)  FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p50_us",
            "(SELECT quantile_cont(edge_score_exec_us, 0.95) FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p95_us",
        ]
    latency_select = (",\n            " + ",\n            ".join(lat_parts)) if lat_parts else ""
    return HEALTH_SNAPSHOT.format(
        rtt_cte=rtt_cte, exec_cte=exec_cte, src=src, ts_pred=ts_pred, latency_select=latency_select
    )


THRESHOLD_PREVIEW_LABELED = """
                WITH sid_scores AS (
                    SELECT edge_sid, MAX(edge_score) AS max_score
                    FROM {src}
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


def query_logs(service_id: str, sql: str, params: tuple = ()) -> list[dict]:
    """Execute ``sql`` against the per-service logs view and return ``list[dict]``."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.core.storage import execute_with_stale_view_retry
    from backend.repositories._base import _compact_sql_for_debug
    from backend.utils.telemetry import get_queries

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    con = None
    t0 = time.monotonic()
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
                "time_ms": round((time.monotonic() - t0) * 1000, 2),
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


_SCORING_COLUMN_TYPES = {
    "edge_score": "DOUBLE",
    "edge_score_l1": "DOUBLE",
    "edge_score_l2": "DOUBLE",
    "edge_sid": "VARCHAR",
    "edge_score_reason": "VARCHAR",
    "edge_cookie_compliance": "VARCHAR",
}


def scoring_source(table: str, cols: set[str]) -> str:
    """A FROM-clause source for ``table`` guaranteed to expose the core scoring columns."""
    if not cols:
        return table
    missing = [(c, t) for c, t in _SCORING_COLUMN_TYPES.items() if c not in cols]
    if not missing:
        return table
    synth = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in missing)
    return f"(SELECT *, {synth} FROM {table})"


def _scoring_table_columns(service_id: str, table: str) -> set[str]:
    """Column-name set for ``table``, or empty set on error."""
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
    """Return per-sid event list within the last ``since_days`` days."""
    if not sids:
        return {}

    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    src = scoring_source(table, _scoring_table_columns(service_id, table))
    placeholders = ",".join("?" for _ in sids)
    where_ts = ts_predicate or f"timestamp >= now() - INTERVAL {int(since_days)} DAY"
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
    """Replay each labeled sid into the ``(session_dict, label)`` tuples that ``evaluate()`` expects."""
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
        scored_values: list[float] = [v for e in events if (v := e.get("edge_score")) is not None]
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
    """Per-sid max-score bucket counts at ``threshold``, joined to ``(sid, label)`` pairs when any exist."""
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
