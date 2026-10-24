"""Per-hour origin-dimension percentile rollups feeding /api/origin/aggregates'
``pop_latency`` + ``ip_health`` + ``path_breakdown`` panels.

All three panels are per-dimension percentile-over-window queries with NO
time-bucket axis — the SAME shape as :mod:`.slow_urls` (per-URL origin
latency) — so they roll up the same way: each closed hour pre-aggregates to
the per-key requests + exact within-hour p50/p95 + (sum, count) for mean
reconstruction. The readers
(:meth:`QueryRunner.try_origin_pop_latency_from_rollup` /
``try_origin_ip_health_from_rollup`` / ``try_origin_path_breakdown_from_rollup``)
request-weight-average the percentiles across hours instead of re-scanning
raw rows.

One module emits THREE files per closed hour (mirrors how
:mod:`.perf_latency` emits perf_top_urls + perf_top_asns from one module —
they differ only in the GROUP-BY key + the per-hour cut):

  ``rollups/hour_bundled/hour=H/origin_pop.parquet``   (key=pop)
  ``rollups/hour_bundled/hour=H/origin_ip.parquet``    (key=oip)
  ``rollups/hour_bundled/hour=H/origin_path.parquet``  (key=edge, 2 rows/hr)

Schemas:

origin_pop.parquet (POP_LATENCY, ``_sql/origin.py``):
  pop             VARCHAR
  requests        BIGINT   -- COUNT(*) for this pop in this hour
  lat_us_count    BIGINT   -- rows with lat_us NOT NULL
  lat_us_sum      DOUBLE   -- SUM(lat_us) for mean reconstruction
  p50_us          DOUBLE   -- MEDIAN(lat_us) exact within this hour
  p95_us          DOUBLE   -- APPROX_QUANTILE(lat_us, 0.95) within hour
top-K=100 by requests (pop cardinality is small; top-100 keeps every pop).

origin_ip.parquet (IP_HEALTH, ``_sql/origin.py``):
  oip             VARCHAR
  requests        BIGINT
  lat_us_count    BIGINT
  lat_us_sum      DOUBLE
  p50_us          DOUBLE
  p95_us          DOUBLE
  ost_5xx_count   BIGINT   -- COUNT(*) FILTER (ost >= 500)
  ost_total_count BIGINT   -- COUNT(*) FILTER (ost IS NOT NULL)
per-hour floor HAVING COUNT(*) >= 5, top-K=100 by requests. error_pct is
EXACT across hours = SUM(ost_5xx_count)/SUM(ost_total_count); only the
percentiles are request-weight-approximate.

origin_path.parquet (PATH_BREAKDOWN, ``_sql/origin.py``):
  edge            BOOLEAN
  requests        BIGINT
  lat_us_count    BIGINT
  lat_us_sum      DOUBLE
  p50_us          DOUBLE
  p95_us          DOUBLE
NO top-K, NO HAVING (only 2 rows per hour).

Percentile-combine across hours is request-weighted-average (DuckDB ships no
sketch-combine; see ``_base.py``) — biased but the ranking is preserved for
the keys that dominate the panel; the response carries ``_approx``.

**Per-hour floor approximation (origin_ip):** the live IP_HEALTH panel applies
a window-level ``HAVING COUNT(*) >= 10`` + ``ORDER BY error_pct DESC``. We
pre-cut each hour at ``COUNT(*) >= ORIGIN_IP_MIN_REQUESTS_PER_HOUR`` (5) and
top-K by requests, then the reader re-applies the window-level
``HAVING SUM(requests) >= 10`` + final ``ORDER BY error_pct DESC``. This is the
same ranking-stable posture as :mod:`.slow_urls`: an IP that dominates the
window is present in enough hours to clear the per-hour floor; a window-rare IP
that never clears 5/hour in any hour is dropped, which is acceptable for a
top-N error leaderboard.

``lat_us`` = ``COALESCE("ottfb", "ttfb"*1000000.0)`` matches
``backend.repositories._base.origin_latency_us_expr`` — replicated verbatim
rather than imported to avoid the import-graph cycle (rollups must NOT import
``repositories``; import-linter enforces this).

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.slow_urls`.
"""

from __future__ import annotations

import logging
import sys

from .. import metric_expressions
from ._common import (
    ORIGIN_DIMS_BUNDLE_TOP_K,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_IP_MIN_REQUESTS_PER_HOUR,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    MultiBundleSpec,
    backfill_missing_bundles,
    backfill_multi_spec_bundles,
    build_multi_spec_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)


def _lat_us_expr(cols: set[str]) -> str | None:
    """Origin latency-us expression. Delegates to the canonical catalog
    (:func:`backend.core.metric_expressions.origin_latency_us`); returns
    ``None`` when no latency column exists.
    """
    return metric_expressions.origin_latency_us(cols, absent=None)


def _build_pop_copy_sql(lat_us_expr: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # POP_LATENCY: key=pop (≠''), MEDIAN/p95 of lat_us, top-K by requests.
    return (
        f"COPY ("
        f"  SELECT "
        f"    pop, "
        f"    CAST(COUNT(*) AS BIGINT) AS requests, "
        f"    CAST(COUNT(*) FILTER (WHERE lat_us IS NOT NULL) AS BIGINT) AS lat_us_count, "
        f"    CAST(COALESCE(SUM(lat_us), 0.0) AS DOUBLE) AS lat_us_sum, "
        f"    CAST(MEDIAN(lat_us) AS DOUBLE) AS p50_us, "
        f"    CAST(APPROX_QUANTILE(lat_us, 0.95) AS DOUBLE) AS p95_us "
        f"  FROM ("
        f'    SELECT "pop" AS pop, {lat_us_expr} AS lat_us '
        f"    FROM {table_ident} "
        f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"      AND timestamp <  TIMESTAMPTZ '{end_iso}' "
        f'      AND "pop" IS NOT NULL AND "pop" != \'\''
        f"  ) "
        f"  WHERE lat_us IS NOT NULL "
        f"  GROUP BY pop "
        f"  ORDER BY COUNT(*) DESC "
        f"  LIMIT {ORIGIN_DIMS_BUNDLE_TOP_K}"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_ip_copy_sql(lat_us_expr: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # IP_HEALTH: key=oip (≠''), p50/p95 of lat_us, carry 5xx + total counts so
    # the reader's error_pct = SUM(5xx)/SUM(total) is EXACT across hours.
    # Per-hour floor HAVING COUNT(*) >= ORIGIN_IP_MIN_REQUESTS_PER_HOUR, top-K
    # by requests.
    return (
        f"COPY ("
        f"  SELECT "
        f"    oip, "
        f"    CAST(COUNT(*) AS BIGINT) AS requests, "
        f"    CAST(COUNT(*) FILTER (WHERE lat_us IS NOT NULL) AS BIGINT) AS lat_us_count, "
        f"    CAST(COALESCE(SUM(lat_us), 0.0) AS DOUBLE) AS lat_us_sum, "
        f"    CAST(MEDIAN(lat_us) AS DOUBLE) AS p50_us, "
        f"    CAST(APPROX_QUANTILE(lat_us, 0.95) AS DOUBLE) AS p95_us, "
        f"    CAST(COUNT(*) FILTER (WHERE ost >= 500) AS BIGINT) AS ost_5xx_count, "
        f"    CAST(COUNT(*) FILTER (WHERE ost IS NOT NULL) AS BIGINT) AS ost_total_count "
        f"  FROM ("
        f'    SELECT "oip" AS oip, "ost" AS ost, {lat_us_expr} AS lat_us '
        f"    FROM {table_ident} "
        f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"      AND timestamp <  TIMESTAMPTZ '{end_iso}' "
        f'      AND "oip" IS NOT NULL AND "oip" != \'\' AND "ost" IS NOT NULL'
        f"  ) "
        f"  GROUP BY oip "
        f"  HAVING COUNT(*) >= {ORIGIN_IP_MIN_REQUESTS_PER_HOUR} "
        f"  ORDER BY COUNT(*) DESC "
        f"  LIMIT {ORIGIN_DIMS_BUNDLE_TOP_K}"
        f") {parquet_zstd_target(tmp_path)}"
    )


def _build_path_copy_sql(lat_us_expr: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # PATH_BREAKDOWN: key=edge (bool), p50/p95 of lat_us. NO top-K, NO HAVING
    # (only 2 rows per hour).
    return (
        f"COPY ("
        f"  SELECT "
        f"    edge, "
        f"    CAST(COUNT(*) AS BIGINT) AS requests, "
        f"    CAST(COUNT(*) FILTER (WHERE lat_us IS NOT NULL) AS BIGINT) AS lat_us_count, "
        f"    CAST(COALESCE(SUM(lat_us), 0.0) AS DOUBLE) AS lat_us_sum, "
        f"    CAST(MEDIAN(lat_us) AS DOUBLE) AS p50_us, "
        f"    CAST(APPROX_QUANTILE(lat_us, 0.95) AS DOUBLE) AS p95_us "
        f"  FROM ("
        f'    SELECT "edge" AS edge, {lat_us_expr} AS lat_us '
        f"    FROM {table_ident} "
        f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"      AND timestamp <  TIMESTAMPTZ '{end_iso}'"
        f"  ) "
        f"  WHERE lat_us IS NOT NULL "
        f"  GROUP BY edge"
        f") {parquet_zstd_target(tmp_path)}"
    )


# (sub_label, output filename, required columns, build_copy_sql). One COPY each.
# The latency column is required for all three; the required columns gate which
# bundle a service can produce (e.g. no ``pop`` → skip the pop bundle but still
# write ip/path).
_ORIGIN_DIMS: tuple[MultiBundleSpec, ...] = (
    ("pop", ORIGIN_POP_BUNDLE_FILENAME, ("pop",), _build_pop_copy_sql),
    ("oip", ORIGIN_IP_BUNDLE_FILENAME, ("oip", "ost"), _build_ip_copy_sql),
    ("edge", ORIGIN_PATH_BUNDLE_FILENAME, ("edge",), _build_path_copy_sql),
)


def _origin_context(cols: set[str], table_ident: str) -> str | None:
    return _lat_us_expr(set(cols))


_common_build_per_hour_bundles = build_per_hour_bundles


def build_origin_dims_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the origin_pop + origin_ip + origin_path rollups for each closed
    hour in ``hours``."""
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_ORIGIN_DIMS,
        group_label="origin_dims",
        tmp_prefix=".tmp_od_",
        logger=logger,
        context_fn=_origin_context,
        driver=build_per_hour_bundles if build_per_hour_bundles is not _common_build_per_hour_bundles else None,
    )


def backfill_origin_dims_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build the origin_dims rollups for every closed hour that
    has ``all_fields.parquet`` but is missing any of the three origin_dims files.
    """
    return backfill_multi_spec_bundles(
        service_id,
        source,
        specs=_ORIGIN_DIMS,
        group_label="origin_dims",
        tmp_prefix=".tmp_od_",
        logger=logger,
        context_fn=_origin_context,
        driver=build_per_hour_bundles if build_per_hour_bundles is not _common_build_per_hour_bundles else None,
    )


def build_origin_summary_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write an origin-summary rollup for each closed hour in ``hours``."""

    def eligibility(cols, table_ident):
        lat_us_expr = metric_expressions.origin_latency_us(cols, absent=None)
        if lat_us_expr is None:
            return None

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
            cdn_ovh_p50_expr = 'CAST(MEDIAN(CAST("elapsed" AS DOUBLE) - CAST("ottlb" AS DOUBLE)) AS DOUBLE)'
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
            f") {parquet_zstd_target(tmp_path)}"
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
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=ORIGIN_SUMMARY_BUNDLE_FILENAME,
        label="origin_summary",
        builder=build_origin_summary_bundles,
        logger=logger,
    )


sys.modules["backend.core.rollups.origin_summary"] = sys.modules[__name__]
