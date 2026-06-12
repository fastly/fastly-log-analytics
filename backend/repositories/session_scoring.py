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

import time as _time

from fastapi import HTTPException


def query_logs(service_id: str, sql: str, params: tuple = ()) -> list[dict]:
    """Execute ``sql`` against the per-service logs view and return
    ``list[dict]``.

    ``params`` is passed through to ``con.execute`` so callers can use
    parametrized queries (e.g. ``WHERE edge_sid IN (?, ?, ?)``) without
    string-formatting user-controlled values into the SQL.
    """
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.repositories._base import _compact_sql_for_debug
    from backend.utils.telemetry import get_queries

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    con = None
    t0 = _time.monotonic()
    try:
        con = get_connection(source=src, max_wait=3, skip_view_update=True, read_only=True)
        rows = con.execute(sql, params).fetchall() if params else con.execute(sql).fetchall()
        cols = [d[0] for d in con.description] if con.description else []
        result = [dict(zip(cols, r)) for r in rows]
        get_queries().append(
            {
                "sql": _compact_sql_for_debug(sql.strip()),
                "time_ms": round((_time.monotonic() - t0) * 1000, 2),
                "rows": len(result),
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
                pass


def fetch_session_events(
    service_id: str,
    sids: list[str],
    since_days: int = 30,
    limit_per_sid: int = 500,
) -> dict[str, list[dict]]:
    """Return ``{sid: [{ts, url, status, ip, ua, edge_score, edge_cookie_compliance, edge_score_reason}, ...]}``
    for every sid in ``sids`` whose events landed in DuckDB within the
    last ``since_days`` days.

    Sids with no rows in the window are dropped from the result. The
    per-sid event cap is a safety bound — a runaway session with 10k+
    requests would otherwise bloat the response; 500 covers any
    realistic browsing pattern.
    """
    if not sids:
        return {}

    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    placeholders = ",".join("?" for _ in sids)
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
            FROM {table}
            WHERE edge_sid IN ({placeholders})
              AND timestamp >= now() - INTERVAL {int(since_days)} DAY
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


def reconstruct_labeled_sessions(service_id: str, labels: list[dict]) -> list[tuple[dict, str]]:
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
    grouped = fetch_session_events(service_id, list(sid_to_label.keys()), since_days=30)
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
