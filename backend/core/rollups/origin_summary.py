"""Per-hour origin-summary rollup writer feeding /api/origin/aggregates'
``summary`` panel.

The summary panel is the headline-card on /origin (total requests, p50/
p95/p99 origin latency, error rate, byte sizes). On wide windows the
underlying ``SUMMARY_ROLLUP`` SQL pays MEDIAN + 4× APPROX_QUANTILE plus
3 MEDIANs on the supplementary columns — ~1.6 s on the active service
at 30 d AFTER the slow_urls rollup landed. Pre-aggregating each closed
hour cuts that to a 30-row read with request-weighted averaging at
read time.

Output: ``rollups/hour_bundled/hour=H/origin_summary.parquet`` with one
row per closed hour:

  requests           BIGINT   -- COUNT(*) for this hour
  total_misses       BIGINT   -- COUNT(*) FILTER (cache ILIKE 'MISS%')
  total_passes       BIGINT   -- COUNT(*) FILTER (cache ILIKE 'PASS%')
  lat_us_count       BIGINT   -- rows with lat_us NOT NULL
  ottfb_p50_us       DOUBLE   -- MEDIAN(lat_us) within hour
  ottfb_p75_us       DOUBLE
  ottfb_p95_us       DOUBLE
  ottfb_p99_us       DOUBLE
  ottlb_count        BIGINT   -- rows with ottlb NOT NULL (0 if no schema col)
  ottlb_p50_us       DOUBLE
  ottlb_p95_us       DOUBLE
  cdn_ovh_count      BIGINT   -- rows with both elapsed+ottlb NOT NULL
  cdn_ovh_p50_us     DOUBLE   -- MEDIAN(elapsed - ottlb)
  ost_5xx_count      BIGINT   -- COUNT(*) FILTER (ost BETWEEN 500 AND 599)
  ost_total_count    BIGINT   -- COUNT(*) FILTER (ost IS NOT NULL)
  obytes_count       BIGINT   -- rows with obytes NOT NULL
  obytes_p50         DOUBLE   -- MEDIAN(obytes)

origin_error_rate aggregates correctly across hours: SUM(ost_5xx_count)
/ SUM(ost_total_count) is the true cross-hour error rate. The percentile
columns aggregate via request-weighted average — same approx posture
as slow_urls (DuckDB can't combine sketches across files; see
``_base.py:1387``).

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.time_series` and :mod:`.slow_urls`.
"""

from __future__ import annotations

import logging

from ._common import (
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_origin_summary_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write an origin-summary rollup for each closed hour in ``hours``.

    Skips the active UTC hour (still being written). Skips services whose
    schema lacks both ``ottfb`` and ``ttfb`` (no latency to summarise).
    Missing optional columns (ottlb, elapsed, obytes, ost, cache) surface
    as 0/NULL in the output so the parquet shape stays uniform across
    services.

    Idempotent — atomic tmp + rename. Returns the number of bundles
    written this call.
    """

    def eligibility(cols, table_ident):
        # Latency expression matches origin_latency_us_expr in _base.py
        # (kept in sync rather than imported to dodge the import cycle).
        if "ottfb" in cols and "ttfb" in cols:
            lat_us_expr = 'COALESCE("ottfb", "ttfb" * 1000000.0)'
        elif "ottfb" in cols:
            lat_us_expr = '"ottfb"'
        elif "ttfb" in cols:
            lat_us_expr = '"ttfb" * 1000000.0'
        else:
            return None

        # Adapt each optional column to whether it exists on this
        # service's schema. Missing → 0/NULL constant so the parquet
        # shape stays uniform.
        miss_expr = (
            "CAST(COUNT(*) FILTER (WHERE \"cache\" ILIKE 'MISS%') AS BIGINT)"
            if "cache" in cols
            else "CAST(0 AS BIGINT)"
        )
        pass_expr = (
            "CAST(COUNT(*) FILTER (WHERE \"cache\" ILIKE 'PASS%') AS BIGINT)"
            if "cache" in cols
            else "CAST(0 AS BIGINT)"
        )
        ottlb_count_expr = (
            'CAST(COUNT(*) FILTER (WHERE "ottlb" IS NOT NULL) AS BIGINT)' if "ottlb" in cols else "CAST(0 AS BIGINT)"
        )
        ottlb_p50_expr = 'CAST(MEDIAN("ottlb") AS DOUBLE)' if "ottlb" in cols else "CAST(NULL AS DOUBLE)"
        ottlb_p95_expr = 'CAST(APPROX_QUANTILE("ottlb", 0.95) AS DOUBLE)' if "ottlb" in cols else "CAST(NULL AS DOUBLE)"
        if "elapsed" in cols and "ottlb" in cols:
            cdn_ovh_count_expr = 'CAST(COUNT(*) FILTER (WHERE "elapsed" IS NOT NULL AND "ottlb" IS NOT NULL) AS BIGINT)'
            cdn_ovh_p50_expr = 'CAST(MEDIAN("elapsed" - "ottlb") AS DOUBLE)'
        else:
            cdn_ovh_count_expr = "CAST(0 AS BIGINT)"
            cdn_ovh_p50_expr = "CAST(NULL AS DOUBLE)"
        if "ost" in cols:
            ost_5xx_expr = 'CAST(COUNT(*) FILTER (WHERE "ost" BETWEEN 500 AND 599) AS BIGINT)'
            ost_total_expr = 'CAST(COUNT(*) FILTER (WHERE "ost" IS NOT NULL) AS BIGINT)'
        else:
            ost_5xx_expr = "CAST(0 AS BIGINT)"
            ost_total_expr = "CAST(0 AS BIGINT)"
        if "obytes" in cols:
            obytes_count_expr = 'CAST(COUNT(*) FILTER (WHERE "obytes" IS NOT NULL) AS BIGINT)'
            obytes_p50_expr = 'CAST(MEDIAN("obytes") AS DOUBLE)'
        else:
            obytes_count_expr = "CAST(0 AS BIGINT)"
            obytes_p50_expr = "CAST(NULL AS DOUBLE)"

        return {
            "lat_us_expr": lat_us_expr,
            "miss_expr": miss_expr,
            "pass_expr": pass_expr,
            "ottlb_count_expr": ottlb_count_expr,
            "ottlb_p50_expr": ottlb_p50_expr,
            "ottlb_p95_expr": ottlb_p95_expr,
            "cdn_ovh_count_expr": cdn_ovh_count_expr,
            "cdn_ovh_p50_expr": cdn_ovh_p50_expr,
            "ost_5xx_expr": ost_5xx_expr,
            "ost_total_expr": ost_total_expr,
            "obytes_count_expr": obytes_count_expr,
            "obytes_p50_expr": obytes_p50_expr,
        }

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        lat_us_expr = ctx["lat_us_expr"]
        miss_expr = ctx["miss_expr"]
        pass_expr = ctx["pass_expr"]
        ottlb_count_expr = ctx["ottlb_count_expr"]
        ottlb_p50_expr = ctx["ottlb_p50_expr"]
        ottlb_p95_expr = ctx["ottlb_p95_expr"]
        cdn_ovh_count_expr = ctx["cdn_ovh_count_expr"]
        cdn_ovh_p50_expr = ctx["cdn_ovh_p50_expr"]
        ost_5xx_expr = ctx["ost_5xx_expr"]
        ost_total_expr = ctx["ost_total_expr"]
        obytes_count_expr = ctx["obytes_count_expr"]
        obytes_p50_expr = ctx["obytes_p50_expr"]
        return (
            f"COPY ("
            f"  SELECT "
            f"    CAST(COUNT(*) AS BIGINT) AS requests, "
            f"    {miss_expr} AS total_misses, "
            f"    {pass_expr} AS total_passes, "
            f"    CAST(COUNT(*) FILTER (WHERE lat_us IS NOT NULL) AS BIGINT) AS lat_us_count, "
            f"    CAST(MEDIAN(lat_us) AS DOUBLE) AS ottfb_p50_us, "
            f"    CAST(APPROX_QUANTILE(lat_us, 0.75) AS DOUBLE) AS ottfb_p75_us, "
            f"    CAST(APPROX_QUANTILE(lat_us, 0.95) AS DOUBLE) AS ottfb_p95_us, "
            f"    CAST(APPROX_QUANTILE(lat_us, 0.99) AS DOUBLE) AS ottfb_p99_us, "
            f"    {ottlb_count_expr} AS ottlb_count, "
            f"    {ottlb_p50_expr} AS ottlb_p50_us, "
            f"    {ottlb_p95_expr} AS ottlb_p95_us, "
            f"    {cdn_ovh_count_expr} AS cdn_ovh_count, "
            f"    {cdn_ovh_p50_expr} AS cdn_ovh_p50_us, "
            f"    {ost_5xx_expr} AS ost_5xx_count, "
            f"    {ost_total_expr} AS ost_total_count, "
            f"    {obytes_count_expr} AS obytes_count, "
            f"    {obytes_p50_expr} AS obytes_p50 "
            f"  FROM ("
            f"    SELECT *, {lat_us_expr} AS lat_us "
            f"    FROM {table_ident} "
            f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"      AND timestamp <  TIMESTAMPTZ '{end_iso}'"
            f"  )"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=ORIGIN_SUMMARY_BUNDLE_FILENAME,
        tmp_prefix=".tmp_os_",
        label="origin_summary",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_origin_summary_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build origin_summary.parquet for every closed
    hour that already has ``all_fields.parquet`` but no
    ``origin_summary.parquet`` yet.

    Mirrors :func:`backfill_slow_urls_bundles`. Idempotent — skips
    already-built hours. Returns the number of bundles written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=ORIGIN_SUMMARY_BUNDLE_FILENAME,
        label="origin_summary",
        builder=build_origin_summary_bundles,
        logger=logger,
    )
