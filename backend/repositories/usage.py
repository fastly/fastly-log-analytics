"""Usage repository — DuckDB queries for log activity, storage stats, and edge ratio."""

from __future__ import annotations

import duckdb

from backend.repositories._base import QueryRunner, _safe_table
from backend.repositories._sql import usage as SQL


def get_edge_ratio(con: duckdb.DuckDBPyConnection, src: dict) -> tuple[float | None, list]:
    """Return (edge_ratio_pct_or_None, debug_queries)."""
    runner = QueryRunner(con, src)
    table = _safe_table(src["name"])
    from backend.core.duckdb import get_schema

    actual_cols = [col["name"] for col in get_schema(con, src)]
    if "edge" not in actual_cols:
        return None, runner.debug_queries
    result = runner.execute_with_retry(SQL.EDGE_RATIO_PCT.format(table=table))
    if result is None:
        return None, runner.debug_queries
    row = result.fetchone()
    ratio = round(float(row[0]), 1) if row and row[0] is not None else None
    return ratio, runner.debug_queries


def get_storage_stats(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_str: str,
    end_str: str,
) -> dict:
    """Return ingested-file count and total bytes for the given time window.

    Window filter is pushed into SQLite (COUNT/SUM against the source_name
    index) so the cost panel doesn't pull every row per service open.
    """
    from backend.core import metadata as metadata_db

    total_files, total_bytes = metadata_db.get_storage_stats_window(src["name"], start_str, end_str)
    return {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "_debug_queries": [],
        "_debug_calls": [],
    }


def get_log_activity(
    src: dict,
    start_str: str,
    end_str: str,
    by: str,
) -> dict:
    """Return time-bucketed log activity (rows and bytes ingested per bucket).

    Reads from the per-service SQLite ``ingested_files`` table — no DuckDB
    work involved. The router no longer asks the deps for a connection,
    which means each call skips one ``get_connection()`` lookup +
    ``update_iceberg_view`` rebind it never actually used.
    """
    from backend.core import metadata as metadata_db

    service_id = src.get("name") or src.get("service_id", "")
    out = metadata_db.get_log_activity(service_id, start_str, end_str, by)
    return {**out, "_debug_queries": [], "_debug_calls": []}
