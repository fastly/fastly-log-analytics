"""Small per-service stat queries over the request-log table (header badge, WAF sanity check).

``table_name`` must already be validated by the caller (``_safe_table_name`` / ``_safe_table``).
"""

from __future__ import annotations

from typing import Any

from backend.repositories._sql import service_stats as sql


def get_request_row_stats(con, table_name: str) -> tuple[int, Any]:
    """``(total rows, newest timestamp)`` for the request table in one scan; newest is ``None`` when empty."""
    row = con.execute(sql.REQUEST_ROW_STATS.format(table=table_name)).fetchone()
    return (row[0], row[1]) if row else (0, None)


def get_waf_flag_counts_7d(con, table_name: str) -> tuple[Any, Any] | None:
    """``(rows with waf = true, total rows)`` over the last 7 days, or ``None`` when no row is returned."""
    result = con.execute(sql.WAF_FLAG_COUNTS_7D.format(table=table_name)).fetchone()
    return (result[0], result[1]) if result else None
