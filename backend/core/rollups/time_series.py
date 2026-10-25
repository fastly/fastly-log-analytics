"""Per-hour 1-minute time-series bundle writer + its backfill driver."""

from __future__ import annotations

import logging
import sys

from .. import metric_expressions
from ._common import (
    OVERVIEW_BUNDLE_FILENAME,
    SESSIONS_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)


def build_time_series_bundles(
    service_id: str, source: dict, hours: list[str], *, output_root: str | None = None
) -> int:
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

    Idempotent (atomic tmp + rename). ``output_root`` overrides only the
    destination for private day-repair staging. All hours share one source
    connection. Returns the number of bundles written this call.
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
            f"{parquet_zstd_target(tmp_path)}"
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
        output_root=output_root,
    )


_HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"


def build_overview_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write an overview rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping overview bundle",
                service_id,
                table_ident,
            )
            return None

        select_parts: list[str] = [
            "CAST(COUNT(*) AS BIGINT) AS requests",
        ]

        has_cache = "cache" in cols
        if has_cache:
            select_parts.append(f"CAST(COUNT(*) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache = 'PASS') AS BIGINT) AS pass_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache = 'SYNTH') AS BIGINT) AS synth_requests")
            select_parts.append(
                "CAST(COUNT(*) FILTER (WHERE cache IN ('MISS', 'PASS', 'SYNTH', 'ERROR')) AS BIGINT) AS origin_requests"
            )
        else:
            select_parts.extend(
                [
                    "CAST(0 AS BIGINT) AS hit_requests",
                    "CAST(0 AS BIGINT) AS miss_requests",
                    "CAST(0 AS BIGINT) AS pass_requests",
                    "CAST(0 AS BIGINT) AS synth_requests",
                    "CAST(0 AS BIGINT) AS origin_requests",
                ]
            )

        if "resp_bytes" in cols and has_cache:
            bw_saved = metric_expressions.sum_or_zero("resp_bytes", where=f"cache IN {_HIT_STATES}")
            select_parts.append(f"CAST({bw_saved} AS BIGINT) AS bandwidth_saved_bytes")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS bandwidth_saved_bytes")

        if "resp_bytes" in cols:
            total_bw = metric_expressions.sum_or_zero("resp_bytes")
            select_parts.append(f"CAST({total_bw} AS BIGINT) AS total_bandwidth_bytes")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS total_bandwidth_bytes")

        if "is_shield" in cols and has_cache:
            select_parts.append(
                f"CAST(COUNT(*) FILTER (WHERE is_shield = true AND cache IN {_HIT_STATES}) AS BIGINT)"
                " AS shield_hit_requests"
            )
            select_parts.append("CAST(COUNT(*) FILTER (WHERE is_shield = true) AS BIGINT) AS shield_total_requests")
        else:
            select_parts.extend(
                [
                    "CAST(0 AS BIGINT) AS shield_hit_requests",
                    "CAST(0 AS BIGINT) AS shield_total_requests",
                ]
            )

        if "waf" in cols and "waf_resp" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS BIGINT) AS threats_blocked")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS threats_blocked")

        if "elapsed" in cols and has_cache:
            hit_el = metric_expressions.sum_or_zero("elapsed", zero="0.0", where=f"cache IN {_HIT_STATES}")
            miss_el = metric_expressions.sum_or_zero("elapsed", zero="0.0", where="cache = 'MISS'")
            select_parts.extend(
                [
                    f"CAST({hit_el} AS DOUBLE) AS hit_elapsed_sum",
                    f"CAST(COUNT(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_elapsed_count",
                    f"CAST({miss_el} AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(COUNT(elapsed) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_elapsed_count",
                ]
            )
        else:
            select_parts.extend(
                [
                    "CAST(0.0 AS DOUBLE) AS hit_elapsed_sum",
                    "CAST(0 AS BIGINT) AS hit_elapsed_count",
                    "CAST(0.0 AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(0 AS BIGINT) AS miss_elapsed_count",
                ]
            )

        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT TIMESTAMPTZ '{start_iso}' AS hour_start, {select_sql} "
            f"FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"AND timestamp < TIMESTAMPTZ '{end_iso}') "
            f"{parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=OVERVIEW_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ov_",
        label="overview",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_overview_bundles(service_id: str, source: dict) -> int:
    """Build overview.parquet for every closed hour that already has
    ``all_fields.parquet`` but no ``overview.parquet``."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=OVERVIEW_BUNDLE_FILENAME,
        label="overview",
        builder=build_overview_bundles,
        logger=logger,
    )


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
            tot_bytes = metric_expressions.sum_or_zero('"resp_bytes"')
            select_parts.append(f"CAST({tot_bytes} AS BIGINT) AS total_bytes")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS total_bytes")
        if "tcp_rtt" in cols:
            rtt_s = metric_expressions.sum_or_zero('"tcp_rtt"', zero="0.0")
            select_parts.append(f"CAST({rtt_s} AS DOUBLE) AS rtt_sum")
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
        if "cmcd_sid" in cols:
            select_parts.append(
                'CAST(COUNT(*) FILTER (WHERE "cmcd_sid" IS NOT NULL AND "cmcd_sid" != \'\') AS BIGINT) AS cmcd_count'
            )
        else:
            select_parts.append("CAST(0 AS BIGINT) AS cmcd_count")

        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} "
            f"FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"AND timestamp < TIMESTAMPTZ '{end_iso}' "
            f'AND "ip" IS NOT NULL '
            f"GROUP BY 1, 2, 3) "
            f"{parquet_zstd_target(tmp_path)}"
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


sys.modules["backend.core.rollups.overview"] = sys.modules[__name__]
sys.modules["backend.core.rollups.sessions"] = sys.modules[__name__]
