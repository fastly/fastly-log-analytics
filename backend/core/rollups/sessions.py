"""Per-hour per-(ip, ja4) sessions bundle writer + its backfill driver."""

from __future__ import annotations

import logging

from ._common import (
    SESSIONS_BUNDLE_FILENAME,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_session_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour ``sessions.parquet`` rollup for each closed hour in
    ``hours``.

    Each row is one ``(ip, ja4)`` group within the hour, holding the
    aggregates the ``/api/sessions`` endpoint needs to render the
    sessions list without re-scanning raw logs:

      bucket           TIMESTAMP -- hour start (UTC, naive — matches
                                   the time_series rollup convention)
      ip               VARCHAR
      ja4              VARCHAR   -- nullable; NULL when the service's
                                   schema has no ja4 column
      first_ts         TIMESTAMP -- MIN(timestamp) for this (ip, ja4, hour)
      last_ts          TIMESTAMP -- MAX(timestamp)
      req_count        BIGINT
      country          VARCHAR   -- MIN(country); nullable
      asn              INTEGER   -- MIN(asn); nullable
      reqs_4xx         BIGINT    -- COUNT(*) WHERE status BETWEEN 400 AND 499
      reqs_5xx         BIGINT    -- COUNT(*) WHERE status >= 500
      total_bytes      BIGINT    -- SUM(resp_bytes)
      rtt_sum          DOUBLE    -- SUM(tcp_rtt), microseconds
      rtt_count        BIGINT    -- COUNT WHERE tcp_rtt IS NOT NULL
      edge_count       BIGINT    -- COUNT WHERE edge = 1
      shield_count     BIGINT    -- COUNT WHERE edge = 0
      ua_min           VARCHAR   -- MIN(ua); cheap stable sample
      edge_sid_max     VARCHAR   -- MAX(edge_sid); representative session id

    Sessions that span multiple hours have a row in each hour bundle —
    the reader stitches by checking that the last_ts of one hour and
    the first_ts of the next for the same (ip, ja4) are within 30 min
    (matching the existing CTE pipeline's session gap threshold).

    Skips the active UTC hour — that hour is still being written and
    the dashboard serves it live. Idempotent via atomic tmp + rename.
    Returns the number of bundles written this call.
    """

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols or "ip" not in cols:
            # No timestamp or no ip → no session boundary, nothing to roll up.
            logger.info(
                "[rollups] %s: skipping sessions bundle (timestamp=%s, ip=%s)",
                service_id,
                "timestamp" in cols,
                "ip" in cols,
            )
            return None

        # Group keys: (ip, ja4) when ja4 exists, else (ip) with NULL ja4.
        # Cast NULL to VARCHAR so the parquet schema is consistent across
        # services regardless of whether ja4 was present at write time.
        ja4_expr = '"ja4"' if "ja4" in cols else "CAST(NULL AS VARCHAR)"

        # Adapt each metric to the service's schema. Missing columns
        # surface as constants so the parquet shape stays uniform across
        # services — same pattern as build_time_series_bundles.
        select_parts = [
            "time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
            'CAST("ip" AS VARCHAR) AS ip',
            f"CAST({ja4_expr} AS VARCHAR) AS ja4",
            "MIN(timestamp) AS first_ts",
            "MAX(timestamp) AS last_ts",
            "CAST(COUNT(*) AS BIGINT) AS req_count",
        ]
        if "country" in cols:
            select_parts.append('CAST(MIN("country") AS VARCHAR) AS country')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS country")
        if "asn" in cols:
            select_parts.append('CAST(MIN("asn") AS INTEGER) AS asn')
        else:
            select_parts.append("CAST(NULL AS INTEGER) AS asn")
        if "status" in cols:
            select_parts.append(
                'CAST(SUM(CASE WHEN "status" BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_4xx'
            )
            select_parts.append('CAST(SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_5xx')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS reqs_4xx")
            select_parts.append("CAST(0 AS BIGINT) AS reqs_5xx")
        if "resp_bytes" in cols:
            select_parts.append('CAST(COALESCE(SUM("resp_bytes"), 0) AS BIGINT) AS total_bytes')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS total_bytes")
        if "tcp_rtt" in cols:
            select_parts.append('CAST(COALESCE(SUM("tcp_rtt"), 0.0) AS DOUBLE) AS rtt_sum')
            select_parts.append('CAST(COUNT(*) FILTER (WHERE "tcp_rtt" IS NOT NULL) AS BIGINT) AS rtt_count')
        else:
            select_parts.append("CAST(0.0 AS DOUBLE) AS rtt_sum")
            select_parts.append("CAST(0 AS BIGINT) AS rtt_count")
        if "edge" in cols:
            select_parts.append('CAST(SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS BIGINT) AS edge_count')
            select_parts.append('CAST(SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS BIGINT) AS shield_count')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS edge_count")
            select_parts.append("CAST(0 AS BIGINT) AS shield_count")
        if "ua" in cols:
            select_parts.append('CAST(MIN("ua") AS VARCHAR) AS ua_min')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS ua_min")
        if "edge_sid" in cols:
            select_parts.append('CAST(MAX("edge_sid") AS VARCHAR) AS edge_sid_max')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS edge_sid_max")

        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} "
            f"FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"AND timestamp < TIMESTAMPTZ '{end_iso}' "
            f'AND "ip" IS NOT NULL '
            f"GROUP BY 1, 2, 3) "
            f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=SESSIONS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_sess_",
        label="sessions",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )
