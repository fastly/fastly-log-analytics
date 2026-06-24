"""Per-hour per-ASN client-speed (``c_speed``) distribution rollup
feeding /api/network-health's ``speed_distribution_query`` panel.

The live panel does ``SELECT asn, c_speed, COUNT(*) FROM <temp> WHERE
asn IN (...) GROUP BY asn, c_speed`` — on prod 30 d that's ~2.9 s
because it scans the full filtered window per render. This writer
pre-aggregates per closed hour to (asn, c_speed, count) rows for
the top-K ASNs by request count.

Unlike :mod:`.network_rtt` the math is EXACT across hours (pure SUM
of integer counts) — no request-weighted average, no ``_approx``
flag needed. The reader simply SUMs the per-(asn, c_speed) counts
across all in-window rollup paths.

Output: ``rollups/hour_bundled/hour=H/network_speed.parquet`` with
at most ``NETWORK_SPEED_BUNDLE_TOP_K`` × ``|c_speed values|`` rows
per hour. Schema:

  asn          BIGINT
  c_speed      VARCHAR
  count        BIGINT

Active-hour skip + atomic tmp+rename + per-service iceberg lock —
same convention as :mod:`.network_rtt`.
"""

from __future__ import annotations

import logging

from ._common import (
    NETWORK_SPEED_BUNDLE_FILENAME,
    NETWORK_SPEED_BUNDLE_MIN_REQUESTS_PER_HOUR,
    NETWORK_SPEED_BUNDLE_TOP_K,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_network_speed_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_speed rollup for each closed hour.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``asn`` or ``c_speed``
      - Hours with no in-hour data

    Idempotent — atomic tmp+rename under the per-service iceberg lock.
    Returns the number of bundles written this call.
    """

    def eligibility(cols, table_ident):
        if "asn" not in cols or "c_speed" not in cols:
            return None
        return True

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        # Two-pass: rank ASNs by total in-hour requests (top-K),
        # then take the (asn, c_speed) distribution for that ASN
        # subset. Caps both row count AND read-time WHERE-prune
        # cost by guaranteeing the rollup holds the same ASN set
        # the leaderboard renders.
        return (
            f"COPY ("
            f"  WITH top_asns AS ("
            f"    SELECT asn FROM {table_ident} "
            f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"      AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"      AND asn IS NOT NULL "
            f"    GROUP BY asn "
            f"    HAVING COUNT(*) >= {NETWORK_SPEED_BUNDLE_MIN_REQUESTS_PER_HOUR} "
            f"    ORDER BY COUNT(*) DESC "
            f"    LIMIT {NETWORK_SPEED_BUNDLE_TOP_K}"
            f"  ) "
            f"  SELECT CAST(t.asn AS BIGINT) AS asn, "
            f"         CAST(t.c_speed AS VARCHAR) AS c_speed, "
            f"         CAST(COUNT(*) AS BIGINT) AS count "
            f"  FROM {table_ident} t "
            f"  INNER JOIN top_asns USING (asn) "
            f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"    AND t.c_speed IS NOT NULL "
            f"    AND t.c_speed != '' "
            f"  GROUP BY t.asn, t.c_speed"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=NETWORK_SPEED_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ns_",
        label="network_speed",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_network_speed_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build network_speed.parquet for every closed hour
    that has all_fields.parquet but no network_speed.parquet yet.

    Mirrors :func:`backfill_network_rtt_bundles`. Idempotent — skips
    already-built hours. Returns the number of bundles written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_SPEED_BUNDLE_FILENAME,
        label="network_speed",
        builder=build_network_speed_bundles,
        logger=logger,
    )
