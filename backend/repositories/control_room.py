"""Control Room queries: recent error events and the correlator's top-N over the request view."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from backend.repositories._sql import control_room as sql


def _record_duckdb_query(sql_str: str, elapsed_ms: float, service_id: str | None = None) -> None:
    try:
        from backend.utils.telemetry import _QUERIES, record_telemetry_query

        compact = " ".join(sql_str.strip().split())
        q_list = _QUERIES.get()
        if q_list is not None:
            q_list.append({"sql": compact, "time_ms": elapsed_ms, "engine": "DuckDB"})
        record_telemetry_query("duckdb", compact, elapsed_ms, service_id=service_id)
    except Exception:
        pass


def recent_standard_error_events(con, view_name: str, *, service_id: str | None = None) -> list[dict]:
    """Last-2-minute 4xx/5xx events (max 20, newest first) from the standard-mode request view."""
    q = sql.RECENT_STANDARD_ERRORS.format(view=view_name)
    t0 = time.perf_counter()
    rows = con.execute(q).fetchall()
    _record_duckdb_query(q, round((time.perf_counter() - t0) * 1000, 2), service_id=service_id)
    return [
        {
            "event_id": str(row[0]),
            "timestamp": row[1].isoformat(),
            "status": int(row[2]),
            "url": str(row[3]),
            "pop": str(row[4]),
            "client_ip": str(row[5]),
        }
        for row in rows
    ]


def recent_high_scale_error_events(client: Any, service_id: str) -> list[dict]:
    """Last-2-minute 4xx/5xx events (max 20, newest first) for a high-scale (ClickHouse) service."""
    rows = client.execute(sql.RECENT_HIGH_SCALE_ERRORS, {"service_id": service_id})
    return [
        {
            "event_id": str(row["event_id"]),
            "timestamp": row["timestamp"].isoformat(),
            "status": int(row["status"]),
            "url": str(row.get("url") or ""),
            "pop": str(row.get("pop") or ""),
            "client_ip": str(row.get("client_ip") or ""),
        }
        for row in rows
    ]


def correlate_top_values(
    con,
    view_name: str,
    col: str,
    since: datetime,
    limit: int,
    *,
    service_id: str | None = None,
) -> tuple[list, Any]:
    """``(top [(value, count)] since ``since``, MAX(timestamp) row)`` for one allow-listed column."""
    q_top = sql.CORRELATE_TOP_VALUES.format(col=col, view=view_name)
    t0 = time.perf_counter()
    rows = con.execute(q_top, [since, limit]).fetchall()
    _record_duckdb_query(q_top, round((time.perf_counter() - t0) * 1000, 2), service_id=service_id)

    q_fresh = sql.LATEST_TIMESTAMP_WINDOWED.format(view=view_name)
    t1 = time.perf_counter()
    freshness_row = con.execute(q_fresh, [since]).fetchone()
    _record_duckdb_query(q_fresh, round((time.perf_counter() - t1) * 1000, 2), service_id=service_id)

    if (not freshness_row or freshness_row[0] is None) and not rows:
        q_fallback = sql.LATEST_TIMESTAMP.format(view=view_name)
        t2 = time.perf_counter()
        freshness_row = con.execute(q_fallback).fetchone()
        _record_duckdb_query(q_fallback, round((time.perf_counter() - t2) * 1000, 2), service_id=service_id)
    return rows, freshness_row
