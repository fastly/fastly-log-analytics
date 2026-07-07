"""Per-hour per-ASN heatmap metrics rollup for /api/network-health.

The live ``heatmap`` section runs ``HEATMAP_BY_ASN_BUCKET`` (a 2-pass CTE
with ``APPROX_QUANTILE`` across the temp table) which dominates the 2–4 s
``create_filtered_temp_table`` scan on 30 d windows. This writer pre-aggregates
each closed hour to a top-K cap of 200 ASNs by request count, storing the
per-hour median RTT, ploss, and error rate so the reader can serve heatmap
rows at hourly bucket granularity without touching the raw Iceberg view.

Output: ``rollups/hour_bundled/hour=H/network_heatmap.parquet`` with K=200
rows per hour, schema:

  asn            BIGINT      -- ASN integer
  hour_ts        TIMESTAMPTZ -- hour start timestamp (bucket in reader)
  reqs           BIGINT      -- COUNT(*) for this ASN in this hour
  errors         BIGINT      -- COUNT(*) FILTER (status >= 400 OR status = 0)
  resp_bytes_sum DOUBLE      -- SUM(resp_bytes) for throughput (bytes/3600 s)
  rtt_p50_us     DOUBLE      -- APPROX_QUANTILE(tcp_rtt, 0.5)
  rtt_min_p50_us DOUBLE      -- APPROX_QUANTILE(rtt_min, 0.5); NULL if absent
  rtt_var_p50_us DOUBLE      -- APPROX_QUANTILE(rtt_var, 0.5); NULL if absent
  rtt_count      BIGINT      -- COUNT(*) FILTER (WHERE tcp_rtt IS NOT NULL)
  ploss_sum      DOUBLE      -- SUM(ploss); NULL if ploss not in schema
  ploss_count    BIGINT      -- COUNT(*) FILTER (WHERE ploss IS NOT NULL)

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as network_rtt / origin_summary.
"""

from __future__ import annotations

import logging

from ._common import (
    NETWORK_HEATMAP_BUNDLE_FILENAME,
    NETWORK_HEATMAP_BUNDLE_MIN_REQUESTS_PER_HOUR,
    NETWORK_HEATMAP_BUNDLE_TOP_K,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_network_heatmap_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_heatmap rollup for each closed hour in ``hours``.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``asn`` or ``tcp_rtt`` (nothing to roll up)
      - Hours with no in-hour data

    Idempotent — atomic tmp + rename under the per-service iceberg lock.
    Returns the number of bundles written this call.
    """

    def eligibility(cols: set[str], table_ident: str) -> dict | None:  # noqa: ARG001
        if "asn" not in cols or "tcp_rtt" not in cols:
            return None
        return {
            "has_rtt_min": "rtt_min" in cols,
            "has_rtt_var": "rtt_var" in cols,
            "has_ploss": "ploss" in cols,
        }

    def build_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        assert isinstance(ctx, dict)
        rtt_min_expr = "CAST(APPROX_QUANTILE(rtt_min, 0.5) AS DOUBLE)" if ctx["has_rtt_min"] else "CAST(NULL AS DOUBLE)"
        rtt_var_expr = "CAST(APPROX_QUANTILE(rtt_var, 0.5) AS DOUBLE)" if ctx["has_rtt_var"] else "CAST(NULL AS DOUBLE)"
        ploss_sum_expr = "CAST(SUM(ploss) AS DOUBLE)" if ctx["has_ploss"] else "CAST(NULL AS DOUBLE)"
        ploss_count_expr = (
            "CAST(COUNT(*) FILTER (WHERE ploss IS NOT NULL) AS BIGINT)" if ctx["has_ploss"] else "CAST(0 AS BIGINT)"
        )
        return (
            f"COPY ("
            f"  SELECT"
            f"    CAST(asn AS BIGINT)                                         AS asn,"
            f"    TIMESTAMPTZ '{start_iso}'                                   AS hour_ts,"
            f"    CAST(COUNT(*) AS BIGINT)                                    AS reqs,"
            f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS BIGINT) AS errors,"
            f"    CAST(SUM(resp_bytes) AS DOUBLE)                             AS resp_bytes_sum,"
            f"    CAST(APPROX_QUANTILE(tcp_rtt, 0.5) AS DOUBLE)              AS rtt_p50_us,"
            f"    {rtt_min_expr}                                              AS rtt_min_p50_us,"
            f"    {rtt_var_expr}                                              AS rtt_var_p50_us,"
            f"    CAST(COUNT(*) FILTER (WHERE tcp_rtt IS NOT NULL) AS BIGINT) AS rtt_count,"
            f"    {ploss_sum_expr}                                            AS ploss_sum,"
            f"    {ploss_count_expr}                                          AS ploss_count"
            f"  FROM {table_ident}"
            f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}'"
            f"    AND timestamp <  TIMESTAMPTZ '{end_iso}'"
            f"    AND asn IS NOT NULL"
            f"    AND tcp_rtt IS NOT NULL AND tcp_rtt > 0"
            f"  GROUP BY asn"
            f"  HAVING COUNT(*) >= {NETWORK_HEATMAP_BUNDLE_MIN_REQUESTS_PER_HOUR}"
            f"  ORDER BY COUNT(*) DESC"
            f"  LIMIT {NETWORK_HEATMAP_BUNDLE_TOP_K}"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=NETWORK_HEATMAP_BUNDLE_FILENAME,
        tmp_prefix=".tmp_nh_",
        label="network_heatmap",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_network_heatmap_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build network_heatmap.parquet for every closed hour
    that has all_fields.parquet but no network_heatmap.parquet yet.

    Mirrors :func:`backfill_network_rtt_bundles`. Idempotent — skips
    already-built hours. Returns the number of bundles written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_HEATMAP_BUNDLE_FILENAME,
        label="network_heatmap",
        builder=build_network_heatmap_bundles,
        logger=logger,
    )
