"""Per-hour POP health rollup feeding /api/network/pop-health.

Pre-aggregates per closed hour to POP-level rows.
Math is exact across hours (SUM of counts), and request-weighted average for percentiles.
Since we need p50_rtt_us and p95_ttfb_ms, we use _approx.

Schema:
  pop             VARCHAR
  requests        BIGINT
  errors          BIGINT
  cache_hits      BIGINT
  bandwidth_bytes BIGINT
  p50_rtt_us      DOUBLE
  p95_ttfb_ms     DOUBLE
"""

from __future__ import annotations

import logging

from ._common import (
    POP_HEALTH_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_pop_health_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    def eligibility(cols, table_ident):
        if "pop" not in cols:
            return None
        return True

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        return (
            f"COPY ("
            f"  SELECT "
            f"      CAST(pop AS VARCHAR) AS pop, "
            f"      CAST(COUNT(*) AS BIGINT) AS requests, "
            f"      CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS BIGINT) AS errors, "
            f"      CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS cache_hits, "
            f"      CAST(SUM(resp_bytes) AS BIGINT) AS bandwidth_bytes, "
            f"      CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS p50_rtt_us, "
            f"      CAST(approx_quantile(ttfb, 0.95) AS DOUBLE) AS p95_ttfb_ms "
            f"  FROM {table_ident} "
            f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"    AND pop IS NOT NULL "
            f"    AND pop != '' "
            f"  GROUP BY pop"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=POP_HEALTH_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ph_",
        label="pop_health",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_pop_health_bundles(service_id: str, source: dict) -> int:
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=POP_HEALTH_BUNDLE_FILENAME,
        label="pop_health",
        builder=build_pop_health_bundles,
        logger=logger,
    )
