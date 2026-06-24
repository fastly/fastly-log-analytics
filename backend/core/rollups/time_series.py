"""Per-hour 1-minute time-series bundle writer + its backfill driver."""

from __future__ import annotations

import logging

from ._common import (
    TIME_SERIES_BUNDLE_FILENAME,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_time_series_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a 1-minute time_series rollup for each closed hour in ``hours``.

    Output: ``rollups/hour_bundled/hour=H/time_series.parquet`` with one row
    per UTC minute and SUM-aggregatable metric columns. Re-bucketing at read
    time to 5/15/60 minutes works as ``SELECT SUM(...) GROUP BY
    time_bucket(...)`` without any sketch.

    Schema (all columns SUM-aggregatable):
      bucket          TIMESTAMP    -- minute floor in UTC
      requests        BIGINT       -- COUNT(*)
      status_4xx      BIGINT       -- COUNT(*) WHERE status BETWEEN 400 AND 499
      status_5xx      BIGINT       -- COUNT(*) WHERE status >= 500
      hits            BIGINT       -- COUNT(*) WHERE cache IN ('HIT','HIT-STALE')
      cache_total     BIGINT       -- COUNT(*) WHERE cache IS NOT NULL
      resp_bytes_sum  BIGINT       -- SUM(resp_bytes)
      ttfb_sum        DOUBLE       -- SUM(ttfb), seconds
      ttfb_count      BIGINT       -- COUNT(*) WHERE ttfb IS NOT NULL

    Columns that map to a backing column missing from this service's
    schema are written as constant 0 so the file shape stays uniform
    across services (the reader uses NULLIF on the denominator).

    Skips the active UTC hour — that hour is still being written and the
    dashboard serves it live off the base table.

    Idempotent (atomic tmp + rename). Returns the number of bundles
    written this call.
    """

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping time_series bundle",
                service_id,
                table_ident,
            )
            return None

        # Build the SELECT, adapting each metric to whether its backing
        # column actually exists on this service's schema. Missing-column
        # rows surface as constant 0 so the parquet shape stays uniform
        # (the reader divides via NULLIF, so 0 cache_total → NULL hit_rate).
        select_parts = [
            "time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
            "CAST(COUNT(*) AS BIGINT) AS requests",
        ]
        if "status" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) AS BIGINT) AS status_4xx")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE status >= 500) AS BIGINT) AS status_5xx")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS status_4xx")
            select_parts.append("CAST(0 AS BIGINT) AS status_5xx")

        if "cache" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS hits")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache IS NOT NULL) AS BIGINT) AS cache_total")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS hits")
            select_parts.append("CAST(0 AS BIGINT) AS cache_total")

        if "resp_bytes" in cols:
            select_parts.append("CAST(COALESCE(SUM(resp_bytes), 0) AS BIGINT) AS resp_bytes_sum")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS resp_bytes_sum")

        if "ttfb" in cols:
            select_parts.append("CAST(COALESCE(SUM(ttfb), 0.0) AS DOUBLE) AS ttfb_sum")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE ttfb IS NOT NULL) AS BIGINT) AS ttfb_count")
        else:
            select_parts.append("CAST(0.0 AS DOUBLE) AS ttfb_sum")
            select_parts.append("CAST(0 AS BIGINT) AS ttfb_count")

        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} "
            f"FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"AND timestamp < TIMESTAMPTZ '{end_iso}' "
            f"GROUP BY 1) "
            f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=TIME_SERIES_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ts_",
        label="time_series",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )
