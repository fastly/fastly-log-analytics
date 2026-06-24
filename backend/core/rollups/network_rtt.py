"""Per-hour per-ASN TCP-RTT percentile rollup feeding the /api/network
-health ``rtt_percentiles_query`` panel.

The live panel computes ``APPROX_QUANTILE(tcp_rtt, 0.95)`` and ``0.99``
per top-N ASN over the requested window — on prod with 30 d the bulk
GROUP BY scans the full network temp table and takes ~5.2 s. This
writer pre-aggregates each closed hour to a top-K cap of ASNs by
request count, with the exact per-hour rtt_count + p95 + p99 baked
in. The reader (:meth:`QueryRunner.try_network_rtt_from_rollup`)
request-weight-averages across hours, biased relative to the true
cross-hour percentile but preserves the per-ASN ranking the FE
leaderboard reads off.

Output: ``rollups/hour_bundled/hour=H/network_rtt.parquet`` with K=100
rows per hour, schema:

  asn          BIGINT   -- ASN integer
  requests     BIGINT   -- COUNT(*) for this ASN in this hour
  rtt_count    BIGINT   -- rows with tcp_rtt > 0 (the weight)
  p95_us       DOUBLE   -- APPROX_QUANTILE(tcp_rtt, 0.95) within hour
  p99_us       DOUBLE   -- APPROX_QUANTILE(tcp_rtt, 0.99) within hour

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.slow_urls` / :mod:`.origin_summary`.
"""

from __future__ import annotations

import logging

from ._common import (
    NETWORK_RTT_BUNDLE_FILENAME,
    NETWORK_RTT_BUNDLE_MIN_REQUESTS_PER_HOUR,
    NETWORK_RTT_BUNDLE_TOP_K,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_network_rtt_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_rtt rollup for each closed hour in ``hours``.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``tcp_rtt`` or ``asn`` (nothing to roll up)
      - Hours with no in-hour data

    Idempotent — atomic tmp + rename under the per-service iceberg
    lock. Returns the number of bundles written this call.
    """

    def eligibility(cols, table_ident):
        if "tcp_rtt" not in cols or "asn" not in cols:
            return None
        return True

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        return (
            f"COPY ("
            f"  SELECT CAST(asn AS BIGINT) AS asn, "
            f"         CAST(COUNT(*) AS BIGINT) AS requests, "
            f"         CAST(COUNT(*) FILTER (WHERE tcp_rtt IS NOT NULL AND tcp_rtt > 0) AS BIGINT) AS rtt_count, "
            f"         CAST(APPROX_QUANTILE(tcp_rtt, 0.95) AS DOUBLE) AS p95_us, "
            f"         CAST(APPROX_QUANTILE(tcp_rtt, 0.99) AS DOUBLE) AS p99_us "
            f"  FROM {table_ident} "
            f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"    AND asn IS NOT NULL "
            f"    AND tcp_rtt IS NOT NULL AND tcp_rtt > 0 "
            f"  GROUP BY asn "
            f"  HAVING COUNT(*) >= {NETWORK_RTT_BUNDLE_MIN_REQUESTS_PER_HOUR} "
            f"  ORDER BY COUNT(*) DESC "
            f"  LIMIT {NETWORK_RTT_BUNDLE_TOP_K}"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=NETWORK_RTT_BUNDLE_FILENAME,
        tmp_prefix=".tmp_nr_",
        label="network_rtt",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_network_rtt_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build network_rtt.parquet for every closed hour
    that has all_fields.parquet but no network_rtt.parquet yet.

    Mirrors :func:`backfill_slow_urls_bundles`. Idempotent — skips
    already-built hours. Returns the number of bundles written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_RTT_BUNDLE_FILENAME,
        label="network_rtt",
        builder=build_network_rtt_bundles,
        logger=logger,
    )
