"""Per-hour per-geocell geo metrics rollup for /api/network-health.

The live ``map_buckets`` / ``cities`` / ``metro_leaderboard`` sections run
``MAP_BY_COUNTRY_BUCKET`` and ``METRO_LEADERBOARD`` against the per-request
temp table, so they always block on the 2–4 s ``create_filtered_temp_table``
scan on 30 d windows. This writer pre-aggregates each closed hour to a row per
(country, city, lat, lon, metro), storing the per-hour request counts, error
counts, RTT sum, and ploss sum so the reader can serve map and metro data at
hourly bucket granularity without touching the raw Iceberg view.

Output: ``rollups/hour_bundled/hour=H/network_geo.parquet`` — one row per
distinct (geocell, hour), schema:

  country     VARCHAR
  city        VARCHAR
  lat         DOUBLE
  lon         DOUBLE
  metro       VARCHAR
  hour_ts     TIMESTAMPTZ -- hour start timestamp (map bucket in reader)
  reqs        BIGINT
  errors      BIGINT      -- COUNT(*) FILTER (status >= 400 OR status = 0)
  ploss_sum   DOUBLE      -- SUM(ploss); NULL if ploss not in schema
  ploss_count BIGINT      -- COUNT(*) FILTER (WHERE ploss IS NOT NULL)
  rtt_sum     DOUBLE      -- SUM(tcp_rtt) for weighted mean at reader
  rtt_count   BIGINT      -- COUNT(*) FILTER (WHERE tcp_rtt IS NOT NULL)

No top-K cap (geo cardinality is bounded by distinct cells, not open-ended).
Min 1 request per cell per hour.

Active-hour skip + atomic tmp+rename — same convention as network_heatmap.
"""

from __future__ import annotations

import logging

from ._common import (
    NETWORK_GEO_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_network_geo_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_geo rollup for each closed hour in ``hours``.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``country`` (nothing to roll up)
      - Hours with no in-hour data

    Idempotent — atomic tmp + rename under the per-service iceberg lock.
    Returns the number of bundles written this call.
    """

    def eligibility(cols: set[str], table_ident: str) -> dict | None:  # noqa: ARG001
        if "country" not in cols:
            return None
        return {
            "has_city": "city" in cols,
            "has_lat": "lat" in cols and "lon" in cols,
            "has_metro": "metro" in cols,
            "has_ploss": "ploss" in cols,
            "has_tcp_rtt": "tcp_rtt" in cols,
        }

    def build_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        assert isinstance(ctx, dict)
        city_col = "CAST(city AS VARCHAR)" if ctx["has_city"] else "CAST('' AS VARCHAR)"
        lat_col = "CAST(lat AS DOUBLE)" if ctx["has_lat"] else "CAST(NULL AS DOUBLE)"
        lon_col = "CAST(lon AS DOUBLE)" if ctx["has_lat"] else "CAST(NULL AS DOUBLE)"
        metro_col = "CAST(metro AS VARCHAR)" if ctx["has_metro"] else "CAST(NULL AS VARCHAR)"
        ploss_sum_expr = "CAST(SUM(ploss) AS DOUBLE)" if ctx["has_ploss"] else "CAST(NULL AS DOUBLE)"
        ploss_count_expr = (
            "CAST(COUNT(*) FILTER (WHERE ploss IS NOT NULL) AS BIGINT)" if ctx["has_ploss"] else "CAST(0 AS BIGINT)"
        )
        rtt_sum_expr = "CAST(SUM(tcp_rtt) AS DOUBLE)" if ctx["has_tcp_rtt"] else "CAST(NULL AS DOUBLE)"
        rtt_count_expr = (
            "CAST(COUNT(*) FILTER (WHERE tcp_rtt IS NOT NULL) AS BIGINT)" if ctx["has_tcp_rtt"] else "CAST(0 AS BIGINT)"
        )
        # GROUP BY must use the same expressions as SELECT. Build the
        # city/lat/lon/metro group keys from the same source columns so
        # DuckDB can resolve them unambiguously.
        city_grp = "city" if ctx["has_city"] else "''"
        lat_grp = "lat" if ctx["has_lat"] else "NULL"
        lon_grp = "lon" if ctx["has_lat"] else "NULL"
        metro_grp = "metro" if ctx["has_metro"] else "NULL"
        return (
            f"COPY ("
            f"  SELECT"
            f"    CAST(country AS VARCHAR)                                       AS country,"
            f"    {city_col}                                                     AS city,"
            f"    {lat_col}                                                      AS lat,"
            f"    {lon_col}                                                      AS lon,"
            f"    {metro_col}                                                    AS metro,"
            f"    TIMESTAMPTZ '{start_iso}'                                     AS hour_ts,"
            f"    CAST(COUNT(*) AS BIGINT)                                      AS reqs,"
            f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS BIGINT) AS errors,"
            f"    {ploss_sum_expr}                                               AS ploss_sum,"
            f"    {ploss_count_expr}                                             AS ploss_count,"
            f"    {rtt_sum_expr}                                                 AS rtt_sum,"
            f"    {rtt_count_expr}                                               AS rtt_count"
            f"  FROM {table_ident}"
            f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}'"
            f"    AND timestamp <  TIMESTAMPTZ '{end_iso}'"
            f"    AND country IS NOT NULL AND country != ''"
            f"  GROUP BY country, {city_grp}, {lat_grp}, {lon_grp}, {metro_grp}"
            f"  HAVING COUNT(*) >= 1"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=NETWORK_GEO_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ng_",
        label="network_geo",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_network_geo_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build network_geo.parquet for every closed hour
    that has all_fields.parquet but no network_geo.parquet yet.

    Mirrors :func:`backfill_network_heatmap_bundles`. Idempotent — skips
    already-built hours. Returns the number of bundles written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_GEO_BUNDLE_FILENAME,
        label="network_geo",
        builder=build_network_geo_bundles,
        logger=logger,
    )
