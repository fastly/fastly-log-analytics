"""Per-hour network-quality dimension rollups feeding /api/network-health's quality panel.

Four files per closed hour:
  rollups/hour_bundled/hour=H/network_quality_country.parquet
  rollups/hour_bundled/hour=H/network_quality_asn.parquet
  rollups/hour_bundled/hour=H/network_quality_region.parquet
  rollups/hour_bundled/hour=H/network_quality_pop.parquet

Output schema for each:
  dim_val      VARCHAR  (the country, asn, region, or pop)
  country      VARCHAR  (only present in region rollup to support country='US' filtering)
  requests     BIGINT   -- COUNT(*) for this dim in this hour (where tcp_rtt > 0)
  p50_us       DOUBLE   -- APPROX_QUANTILE(tcp_rtt, 0.5)

Top-K is capped at 100 per file by requests DESC to save space (UI only needs 25).
"""

from __future__ import annotations

import logging

from ._common import (
    NETWORK_QUALITY_ASN_FILENAME,
    NETWORK_QUALITY_COUNTRY_FILENAME,
    NETWORK_QUALITY_POP_FILENAME,
    NETWORK_QUALITY_REGION_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def _build_sql(
    dim_col: str, include_country: bool, table_ident: str, start_iso: str, end_iso: str, tmp_path: str
) -> str:
    country_select = "CAST(country AS VARCHAR) AS country," if include_country else ""
    country_group = ", country" if include_country else ""
    return (
        f"COPY ("
        f"  SELECT "
        f'    CAST("{dim_col}" AS VARCHAR) AS dim_val, '
        f"    {country_select} "
        f"    CAST(COUNT(*) AS BIGINT) AS requests, "
        f"    CAST(APPROX_QUANTILE(tcp_rtt, 0.5) AS DOUBLE) AS p50_us "
        f"  FROM {table_ident} "
        f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
        f"    AND tcp_rtt IS NOT NULL AND tcp_rtt > 0 "
        f'    AND "{dim_col}" IS NOT NULL AND CAST("{dim_col}" AS VARCHAR) != \'\' '
        f'  GROUP BY "{dim_col}" {country_group} '
        f"  ORDER BY requests DESC "
        f"  LIMIT 100"
        f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def _country_sql(ctx, table, start, end, tmp):
    return _build_sql("country", False, table, start, end, tmp)


def _asn_sql(ctx, table, start, end, tmp):
    return _build_sql("asn", False, table, start, end, tmp)


def _region_sql(ctx, table, start, end, tmp):
    return _build_sql("region", True, table, start, end, tmp)


def _pop_sql(ctx, table, start, end, tmp):
    return _build_sql("pop", False, table, start, end, tmp)


_DIMS = (
    ("quality_country", NETWORK_QUALITY_COUNTRY_FILENAME, ("country", "tcp_rtt"), _country_sql),
    ("quality_asn", NETWORK_QUALITY_ASN_FILENAME, ("asn", "tcp_rtt"), _asn_sql),
    ("quality_region", NETWORK_QUALITY_REGION_FILENAME, ("region", "country", "tcp_rtt"), _region_sql),
    ("quality_pop", NETWORK_QUALITY_POP_FILENAME, ("pop", "tcp_rtt"), _pop_sql),
)


def build_network_quality_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    written = 0
    for label, filename, req_cols, build_copy_sql in _DIMS:

        def eligibility(cols, table_ident, req_cols=req_cols):
            for c in req_cols:
                if c not in cols:
                    return None
            return True

        written += build_per_hour_bundles(
            service_id,
            source,
            hours,
            bundle_filename=filename,
            tmp_prefix=".tmp_nq_",
            label=label,
            eligibility=eligibility,
            build_copy_sql=build_copy_sql,
            logger=logger,
        )
    return written


def backfill_network_quality_bundles(service_id: str, source: dict) -> int:
    written = 0
    for label, filename, req_cols, build_copy_sql in _DIMS:
        written += backfill_missing_bundles(
            service_id,
            source,
            bundle_filename=filename,
            label=label,
            builder=build_network_quality_bundles,
            logger=logger,
        )
    return written
