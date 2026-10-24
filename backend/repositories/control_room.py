"""Control Room queries: recent error events and the correlator's top-N over the request view."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from backend.repositories._base import record_duckdb_query

logger = logging.getLogger(__name__)

RECENT_STANDARD_ERRORS = """
SELECT
    md5(concat_ws('|', CAST(timestamp AS VARCHAR), CAST(status AS VARCHAR),
                  COALESCE(url, ''), COALESCE(pop, ''), COALESCE(ip, ''),
                  CAST(row_number() OVER (
                      ORDER BY timestamp DESC, status, url, pop, ip
                  ) AS VARCHAR))) AS event_id,
    timestamp,
    TRY_CAST(status AS INTEGER) AS status,
    COALESCE(url, '') AS url,
    COALESCE(pop, '') AS pop,
    COALESCE(ip, '') AS client_ip
FROM {view}
WHERE timestamp >= now() - INTERVAL 2 MINUTE
  AND TRY_CAST(status AS INTEGER) BETWEEN 400 AND 599
ORDER BY timestamp DESC
LIMIT 20
"""

# ClickHouse (high-scale) dialect; ``{service_id:String}`` is a ClickHouse bind parameter, not a format field.
RECENT_HIGH_SCALE_ERRORS = """
SELECT
    toString(event_id) AS event_id,
    event_timestamp AS timestamp,
    toInt32OrZero(custom_fields['status']) AS status,
    url,
    custom_fields['pop'] AS pop,
    client_ip
FROM request_facts
WHERE service_id={service_id:String}
  AND event_timestamp >= now() - INTERVAL 2 MINUTE
  AND toInt32OrZero(custom_fields['status']) BETWEEN 400 AND 599
  AND publication_state = 'visible'
ORDER BY event_timestamp DESC
LIMIT 20
"""

CORRELATE_TOP_VALUES = """
SELECT {col} AS value, COUNT(*) AS count
FROM {view}
WHERE timestamp >= ?
GROUP BY 1
ORDER BY 2 DESC
LIMIT ?
"""

LATEST_TIMESTAMP_WINDOWED = "SELECT MAX(timestamp) AS latest FROM {view} WHERE timestamp >= ?"

LATEST_TIMESTAMP = "SELECT MAX(timestamp) AS latest FROM {view}"


def recent_standard_error_events(con, view_name: str, *, service_id: str | None = None) -> list[dict]:
    """Last-2-minute 4xx/5xx events (max 20, newest first) from the standard-mode request view."""
    q = RECENT_STANDARD_ERRORS.format(view=view_name)
    t0 = time.perf_counter()
    rows = con.execute(q).fetchall()
    record_duckdb_query(q, round((time.perf_counter() - t0) * 1000, 2), service_id=service_id)
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
    rows = client.execute(RECENT_HIGH_SCALE_ERRORS, {"service_id": service_id})
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
    q_top = CORRELATE_TOP_VALUES.format(col=col, view=view_name)
    t0 = time.perf_counter()
    rows = con.execute(q_top, [since, limit]).fetchall()
    record_duckdb_query(q_top, round((time.perf_counter() - t0) * 1000, 2), service_id=service_id)

    q_fresh = LATEST_TIMESTAMP_WINDOWED.format(view=view_name)
    t1 = time.perf_counter()
    freshness_row = con.execute(q_fresh, [since]).fetchone()
    record_duckdb_query(q_fresh, round((time.perf_counter() - t1) * 1000, 2), service_id=service_id)

    if (not freshness_row or freshness_row[0] is None) and not rows:
        q_fallback = LATEST_TIMESTAMP.format(view=view_name)
        t2 = time.perf_counter()
        freshness_row = con.execute(q_fallback).fetchone()
        record_duckdb_query(q_fallback, round((time.perf_counter() - t2) * 1000, 2), service_id=service_id)
    return rows, freshness_row
