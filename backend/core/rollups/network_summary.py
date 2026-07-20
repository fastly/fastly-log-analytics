"""Per-hour network-summary rollup writer + backfill driver.

Produces ``rollups/hour_bundled/hour=H/network_summary.parquet`` with
one row per closed UTC hour and SUM-aggregatable count columns covering
the network section of ``/api/value/summary``.

Schema (all columns SUM-aggregatable):
  hour_start             TIMESTAMPTZ  -- hour floor in UTC
  requests               BIGINT       -- COUNT(*)
  http3_requests         BIGINT       -- COUNT(*) WHERE protocol = 'HTTP/3'
  h2_requests            BIGINT       -- COUNT(*) WHERE protocol = 'HTTP/2'
  h11_requests           BIGINT       -- COUNT(*) WHERE protocol = 'HTTP/1.1'
  other_proto_requests   BIGINT       -- everything else
  tls_requests           BIGINT       -- COUNT(*) WHERE is_ssl = true
  ipv6_requests          BIGINT       -- COUNT(*) WHERE ipv6 = true

Missing-column services get constant 0 so the parquet shape stays
uniform. The reader computes derived percentages from the window-wide
SUMs.
"""

from __future__ import annotations

import logging

from ._common import (
    NETWORK_SUMMARY_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_network_summary_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a network-summary rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping network_summary bundle",
                service_id,
                table_ident,
            )
            return None

        select_parts: list[str] = [
            "CAST(COUNT(*) AS BIGINT) AS requests",
        ]

        has_protocol = "protocol" in cols
        if has_protocol:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE protocol = 'HTTP/3') AS BIGINT) AS http3_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE protocol = 'HTTP/2') AS BIGINT) AS h2_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE protocol = 'HTTP/1.1') AS BIGINT) AS h11_requests")
            select_parts.append(
                "CAST(COUNT(*) FILTER (WHERE protocol NOT IN ('HTTP/3', 'HTTP/2', 'HTTP/1.1')) AS BIGINT)"
                " AS other_proto_requests"
            )
        else:
            select_parts.extend(
                [
                    "CAST(0 AS BIGINT) AS http3_requests",
                    "CAST(0 AS BIGINT) AS h2_requests",
                    "CAST(0 AS BIGINT) AS h11_requests",
                    "CAST(0 AS BIGINT) AS other_proto_requests",
                ]
            )

        if "is_ssl" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE is_ssl = true) AS BIGINT) AS tls_requests")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS tls_requests")

        if "ipv6" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE ipv6 = true) AS BIGINT) AS ipv6_requests")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS ipv6_requests")

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
        bundle_filename=NETWORK_SUMMARY_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ns_",
        label="network_summary",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_network_summary_bundles(service_id: str, source: dict) -> int:
    """Build network_summary.parquet for every closed hour that already has
    ``all_fields.parquet`` but no ``network_summary.parquet``."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_SUMMARY_BUNDLE_FILENAME,
        label="network_summary",
        builder=build_network_summary_bundles,
        logger=logger,
    )
