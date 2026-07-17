"""Per-hour overview rollup writer + backfill driver.

Produces ``rollups/hour_bundled/hour=H/overview.parquet`` with one row
per closed UTC hour and SUM-aggregatable metric columns covering the
combined overview + caching sections of ``/api/value/summary``.

Schema (all columns SUM-aggregatable):
  hour_start             TIMESTAMPTZ  -- hour floor in UTC
  requests               BIGINT       -- COUNT(*)
  hit_requests           BIGINT       -- COUNT(*) WHERE cache IN HIT_STATES
  miss_requests          BIGINT       -- COUNT(*) WHERE cache = 'MISS'
  pass_requests          BIGINT       -- COUNT(*) WHERE cache = 'PASS'
  synth_requests         BIGINT       -- COUNT(*) WHERE cache = 'SYNTH'
  origin_requests        BIGINT       -- COUNT(*) WHERE cache IN (MISS,PASS,SYNTH,ERROR)
  bandwidth_saved_bytes  BIGINT       -- SUM(resp_bytes) for hits
  total_bandwidth_bytes  BIGINT       -- SUM(resp_bytes)
  shield_hit_requests    BIGINT       -- shield-specific hit count
  shield_total_requests  BIGINT       -- shield total
  threats_blocked        BIGINT       -- waf=1 AND waf_resp=406
  hit_elapsed_sum        DOUBLE       -- SUM(elapsed) for hits
  hit_elapsed_count      BIGINT       -- COUNT(elapsed) for hits
  miss_elapsed_sum       DOUBLE       -- SUM(elapsed) for misses
  miss_elapsed_count     BIGINT       -- COUNT(elapsed) for misses

Missing-column services get constant 0 so the parquet shape stays
uniform. The reader re-buckets via ``time_bucket(interval, hour_start)``
and computes derived metrics (offload_pct, accel_factor) from the
window-wide SUMs.
"""

from __future__ import annotations

import logging

from ._common import (
    OVERVIEW_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)

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
            select_parts.append(
                f"CAST(COALESCE(SUM(resp_bytes) FILTER (WHERE cache IN {_HIT_STATES}), 0) AS BIGINT)"
                " AS bandwidth_saved_bytes"
            )
        else:
            select_parts.append("CAST(0 AS BIGINT) AS bandwidth_saved_bytes")

        if "resp_bytes" in cols:
            select_parts.append("CAST(COALESCE(SUM(resp_bytes), 0) AS BIGINT) AS total_bandwidth_bytes")
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
            select_parts.extend(
                [
                    f"CAST(COALESCE(SUM(elapsed) FILTER (WHERE cache IN {_HIT_STATES}), 0.0) AS DOUBLE)"
                    " AS hit_elapsed_sum",
                    f"CAST(COUNT(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_elapsed_count",
                    "CAST(COALESCE(SUM(elapsed) FILTER (WHERE cache = 'MISS'), 0.0) AS DOUBLE) AS miss_elapsed_sum",
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
            f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
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
