"""Per-hour network rollup writers and backfill drivers.

Consolidates:
- pop_health (`pop_health.parquet`)
- network_quality (`network_quality_{country,asn,region,pop}.parquet`)
- network_rtt (`network_rtt.parquet`)
- network_summary (`network_summary.parquet`)
- network_speed (`network_speed.parquet`)
- network_heatmap (`network_heatmap.parquet`)
- network_geo (`network_geo.parquet`)
"""

from __future__ import annotations

import logging
import sys

from ._common import (
    NETWORK_GEO_BUNDLE_FILENAME,
    NETWORK_HEATMAP_BUNDLE_FILENAME,
    NETWORK_HEATMAP_BUNDLE_MIN_REQUESTS_PER_HOUR,
    NETWORK_HEATMAP_BUNDLE_TOP_K,
    NETWORK_QUALITY_ASN_FILENAME,
    NETWORK_QUALITY_COUNTRY_FILENAME,
    NETWORK_QUALITY_POP_FILENAME,
    NETWORK_QUALITY_REGION_FILENAME,
    NETWORK_RTT_BUNDLE_FILENAME,
    NETWORK_RTT_BUNDLE_MIN_REQUESTS_PER_HOUR,
    NETWORK_RTT_BUNDLE_TOP_K,
    NETWORK_SPEED_BUNDLE_FILENAME,
    NETWORK_SPEED_BUNDLE_MIN_REQUESTS_PER_HOUR,
    NETWORK_SPEED_BUNDLE_TOP_K,
    NETWORK_SUMMARY_BUNDLE_FILENAME,
    POP_HEALTH_BUNDLE_FILENAME,
    MultiBundleSpec,
    backfill_missing_bundles,
    backfill_multi_spec_bundles,
    build_multi_spec_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)


# ── POP health ───────────────────────────────────────────────────────────────


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
            f") {parquet_zstd_target(tmp_path)}"
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


# ── Network quality ──────────────────────────────────────────────────────────


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
        f") {parquet_zstd_target(tmp_path)}"
    )


def _country_sql(ctx, table, start, end, tmp):
    return _build_sql("country", False, table, start, end, tmp)


def _asn_sql(ctx, table, start, end, tmp):
    return _build_sql("asn", False, table, start, end, tmp)


def _region_sql(ctx, table, start, end, tmp):
    return _build_sql("region", True, table, start, end, tmp)


def _pop_sql(ctx, table, start, end, tmp):
    return _build_sql("pop", False, table, start, end, tmp)


_DIMS: tuple[MultiBundleSpec, ...] = (
    ("country", NETWORK_QUALITY_COUNTRY_FILENAME, ("country", "tcp_rtt"), _country_sql),
    ("asn", NETWORK_QUALITY_ASN_FILENAME, ("asn", "tcp_rtt"), _asn_sql),
    ("region", NETWORK_QUALITY_REGION_FILENAME, ("region", "country", "tcp_rtt"), _region_sql),
    ("pop", NETWORK_QUALITY_POP_FILENAME, ("pop", "tcp_rtt"), _pop_sql),
)


def build_network_quality_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_DIMS,
        group_label="network_quality",
        tmp_prefix=".tmp_nq_",
        logger=logger,
        driver=build_per_hour_bundles,
    )


def backfill_network_quality_bundles(service_id: str, source: dict) -> int:
    return backfill_multi_spec_bundles(
        service_id,
        source,
        specs=_DIMS,
        group_label="network_quality",
        tmp_prefix=".tmp_nq_",
        logger=logger,
        driver=build_per_hour_bundles,
    )


# ── Network RTT ──────────────────────────────────────────────────────────────


def build_network_rtt_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_rtt rollup for each closed hour in ``hours``."""

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
            f") {parquet_zstd_target(tmp_path)}"
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
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_RTT_BUNDLE_FILENAME,
        label="network_rtt",
        builder=build_network_rtt_bundles,
        logger=logger,
    )


# ── Network summary ──────────────────────────────────────────────────────────


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
            f"{parquet_zstd_target(tmp_path)}"
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
    """Self-heal pass: build network_summary.parquet for every closed hour."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_SUMMARY_BUNDLE_FILENAME,
        label="network_summary",
        builder=build_network_summary_bundles,
        logger=logger,
    )


# ── Network speed ────────────────────────────────────────────────────────────


def build_network_speed_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_speed rollup for each closed hour."""

    def eligibility(cols, table_ident):
        if "asn" not in cols or "c_speed" not in cols:
            return None
        return True

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
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
            f") {parquet_zstd_target(tmp_path)}"
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
    """Self-heal pass: build network_speed.parquet for every closed hour."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_SPEED_BUNDLE_FILENAME,
        label="network_speed",
        builder=build_network_speed_bundles,
        logger=logger,
    )


# ── Network heatmap ──────────────────────────────────────────────────────────


def build_network_heatmap_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_heatmap rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> dict | None:  # noqa: ARG001
        if "asn" not in cols or "tcp_rtt" not in cols:
            return None
        return {
            "has_rtt_min": "rtt_min" in cols,
            "has_rtt_var": "rtt_var" in cols,
            "has_ploss": "ploss" in cols,
        }

    def build_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        if not isinstance(ctx, dict):
            raise RuntimeError("isinstance(ctx, dict) must hold")
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
            f") {parquet_zstd_target(tmp_path)}"
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
    """Self-heal pass: build network_heatmap.parquet for every closed hour."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_HEATMAP_BUNDLE_FILENAME,
        label="network_heatmap",
        builder=build_network_heatmap_bundles,
        logger=logger,
    )


# ── Network geo ──────────────────────────────────────────────────────────────


def build_network_geo_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour network_geo rollup for each closed hour in ``hours``."""

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
        if not isinstance(ctx, dict):
            raise RuntimeError("isinstance(ctx, dict) must hold")
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
            f") {parquet_zstd_target(tmp_path)}"
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
    """Self-heal pass: build network_geo.parquet for every closed hour."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=NETWORK_GEO_BUNDLE_FILENAME,
        label="network_geo",
        builder=build_network_geo_bundles,
        logger=logger,
    )


for _alias in (
    "pop_health",
    "network_quality",
    "network_rtt",
    "network_summary",
    "network_speed",
    "network_health_heatmap",
    "network_health_geo",
):
    sys.modules[f"backend.core.rollups.{_alias}"] = sys.modules[__name__]
