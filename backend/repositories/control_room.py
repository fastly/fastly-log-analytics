"""Control Room queries: recent error events and the correlator's top-N over the request view."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.repositories._sql import control_room as sql


def recent_standard_error_events(con, view_name: str) -> list[dict]:
    """Last-2-minute 4xx/5xx events (max 20, newest first) from the standard-mode request view."""
    rows = con.execute(sql.RECENT_STANDARD_ERRORS.format(view=view_name)).fetchall()
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


def correlate_top_values(con, view_name: str, col: str, since: datetime, limit: int) -> tuple[list, Any]:
    """``(top [(value, count)] since ``since``, MAX(timestamp) row)`` for one allow-listed column."""
    rows = con.execute(sql.CORRELATE_TOP_VALUES.format(col=col, view=view_name), [since, limit]).fetchall()
    freshness_row = con.execute(sql.LATEST_TIMESTAMP.format(view=view_name)).fetchone()
    return rows, freshness_row
