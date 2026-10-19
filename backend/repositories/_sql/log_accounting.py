"""SQL templates for `backend.repositories.log_accounting`.

Placeholders are code-generated values only (date_trunc unit, strftime format, quoted parquet
path lists, ISO timestamps) — never request input.
"""

from __future__ import annotations

ROLLUP_BUCKET_COUNTS = (
    "SELECT strftime(date_trunc('{unit}', hour_start), '{fmt}') AS bucket, "
    "  SUM(requests) AS n "
    "FROM read_parquet([{paths_sql}]) "
    "WHERE hour_start >= TIMESTAMPTZ '{start_tz}' "
    "  AND hour_start < TIMESTAMPTZ '{end_tz}' "
    "GROUP BY date_trunc('{unit}', hour_start)"
)

ACTIVE_HOUR_BUCKET_COUNTS = (
    "SELECT strftime(date_trunc('{unit}', timestamp), '{fmt}') AS bucket, COUNT(*) AS n "
    "FROM read_parquet([{paths_sql}], union_by_name=true) "
    "WHERE timestamp >= TIMESTAMP '{start_iso}' "
    "  AND timestamp <  TIMESTAMP '{end_iso}' "
    "GROUP BY date_trunc('{unit}', timestamp)"
)

RAW_BUCKET_COUNTS = (
    "SELECT strftime(date_trunc('{unit}', timestamp), '{fmt}') AS bucket, COUNT(*) AS n "
    "FROM {table_name} "
    "WHERE timestamp >= TIMESTAMP '{start_iso}' "
    "  AND timestamp <  TIMESTAMP '{end_iso}' "
    "GROUP BY date_trunc('{unit}', timestamp)"
)
