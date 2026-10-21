"""Row-count-by-bucket queries behind ingest log accounting (our rows vs Fastly ``requests``)."""

from __future__ import annotations

from backend.repositories._sql import log_accounting as sql
from backend.utils.telemetry import track_query


def rollup_bucket_counts(con, *, unit: str, fmt: str, paths_sql: str, start_tz: str, end_tz: str) -> list[tuple]:
    """Closed-hour request counts per bucket from the overview rollup parquet files."""
    q = sql.ROLLUP_BUCKET_COUNTS.format(unit=unit, fmt=fmt, paths_sql=paths_sql, start_tz=start_tz, end_tz=end_tz)
    with track_query(con, q, [], "log_accounting.rollup_bucket_counts") as cur:
        return cur.fetchall()


def active_hour_bucket_counts(con, *, unit: str, fmt: str, paths_sql: str, start_iso: str, end_iso: str) -> list[tuple]:
    """Row counts per bucket straight from the active-hour committed + buffer parquet files."""
    q = sql.ACTIVE_HOUR_BUCKET_COUNTS.format(
        unit=unit, fmt=fmt, paths_sql=paths_sql, start_iso=start_iso, end_iso=end_iso
    )
    with track_query(con, q, [], "log_accounting.active_hour_bucket_counts") as cur:
        return cur.fetchall()


def raw_bucket_counts(con, *, unit: str, fmt: str, table_name: str, start_iso: str, end_iso: str) -> list[tuple]:
    """Full raw-table scan fallback for row counts per bucket."""
    q = sql.RAW_BUCKET_COUNTS.format(unit=unit, fmt=fmt, table_name=table_name, start_iso=start_iso, end_iso=end_iso)
    with track_query(con, q, [], "log_accounting.raw_bucket_counts") as cur:
        return cur.fetchall()
