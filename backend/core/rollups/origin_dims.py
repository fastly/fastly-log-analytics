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

Active-hour skip + atomic tmp+rename + per-service view lock — same
convention as :mod:`.slow_urls`.
"""

from __future__ import annotations

import logging
from typing import cast

from backend.utils.module_shim import install_shim_module

from .. import metric_expressions
from ._common import (
    ORIGIN_DIMS_BUNDLE_TOP_K,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_IP_MIN_REQUESTS_PER_HOUR,
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    PERF_ASNS_MIN_REQUESTS_PER_HOUR,
    PERF_LATENCY_BUNDLE_TOP_K,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_TTL_DIST_BUNDLE_FILENAME,
    PERF_URLS_MIN_REQUESTS_PER_HOUR,
    SLOW_URLS_BUNDLE_FILENAME,
    SLOW_URLS_BUNDLE_MIN_REQUESTS_PER_HOUR,
    SLOW_URLS_BUNDLE_TOP_K,
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


def _ttfb_lat_expr(cols: set[str]) -> str | None:
    """ttfb latency-us expression. Delegates to the canonical catalog
    (:func:`backend.core.metric_expressions.origin_latency_us`); returns
    ``None`` when no ttfb-latency column exists.
    """
    return metric_expressions.origin_latency_us(cols, absent=None)


def build_origin_latency_ts_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour minute-granular origin_latency_ts rollup for each
    closed hour in ``hours``.

    Skips:
      - The active UTC hour (still being written).
      - Services whose schema lacks BOTH ``ottfb`` and ``ttfb`` (no ttfb
        latency to roll up — nothing to serve).
      - Hours with no matching data (writes a valid empty parquet).

    When ``ottlb`` is absent the ttlb_* columns are written as a constant
    ``count=0`` + ``NULL`` percentile block so the schema stays uniform.

    Idempotent — atomic tmp+rename under the per-service view lock.
    Returns the number of bundles written this call.
    """

    def eligibility(cols, table_ident):
        ttfb_lat = _ttfb_lat_expr(set(cols))
        if ttfb_lat is None:
            return None
        ttlb_lat = '"ottlb"' if "ottlb" in cols else None
        return (ttfb_lat, ttlb_lat)

    def build_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        ttfb_lat, ttlb_lat = cast("tuple[str, str | None]", ctx)
        # ttlb may be absent → emit the uniform 0/NULL block so the parquet
        # schema is identical across services and the reader never references a
        # column that doesn't exist (matches time_series.py's uniform-shape
        # posture).
        if ttlb_lat is not None:
            ttlb_select = (
                f"    CAST(COUNT(*) FILTER (WHERE {ttlb_lat} IS NOT NULL) AS BIGINT) AS ttlb_count, "
                f"    CAST(MEDIAN({ttlb_lat}) AS DOUBLE) AS ttlb_p50_us, "
                f"    CAST(APPROX_QUANTILE({ttlb_lat}, 0.95) AS DOUBLE) AS ttlb_p95_us, "
                f"    CAST(APPROX_QUANTILE({ttlb_lat}, 0.99) AS DOUBLE) AS ttlb_p99_us "
            )
        else:
            ttlb_select = (
                "    CAST(0 AS BIGINT) AS ttlb_count, "
                "    CAST(NULL AS DOUBLE) AS ttlb_p50_us, "
                "    CAST(NULL AS DOUBLE) AS ttlb_p95_us, "
                "    CAST(NULL AS DOUBLE) AS ttlb_p99_us "
            )
        return (
            f"COPY ("
            f"  SELECT "
            f"    CAST(date_trunc('minute', timestamp) AS TIMESTAMPTZ) AS bucket_ts, "
            f"    CAST(COUNT(*) FILTER (WHERE {ttfb_lat} IS NOT NULL) AS BIGINT) AS ttfb_count, "
            f"    CAST(MEDIAN({ttfb_lat}) AS DOUBLE) AS ttfb_p50_us, "
            f"    CAST(APPROX_QUANTILE({ttfb_lat}, 0.95) AS DOUBLE) AS ttfb_p95_us, "
            f"    CAST(APPROX_QUANTILE({ttfb_lat}, 0.99) AS DOUBLE) AS ttfb_p99_us, "
            f"{ttlb_select}"
            f"  FROM {table_ident} "
            f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"  GROUP BY 1"
            f") {parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_olts_",
        label="origin_latency_ts",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_origin_latency_ts_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build origin_latency_ts.parquet for every closed hour
    that has all_fields.parquet but no origin_latency_ts.parquet yet.

    Mirrors :func:`backfill_verified_bots_ts_bundles`. Idempotent — skips
    already-built hours. Returns the number of bundles written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
        label="origin_latency_ts",
        builder=build_origin_latency_ts_bundles,
        logger=logger,
    )


def backfill_slow_urls_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build slow_urls.parquet for every closed hour
    that already has an ``all_fields.parquet`` (per-field rollup) but
    no ``slow_urls.parquet`` yet.

    The slow_urls rollup shipped after the count rollup was already
    backfilled for most services. On a service with months of history,
    walking every closed hour and (re-)issuing a per-hour COPY is what
    populates the per-URL p95 panel without waiting weeks for the live
    cron tick to organically rebuild each hour.

    Idempotent — skips hours whose slow_urls.parquet already exists.
    Returns the number of bundles written.
    """
    # The "all_fields.parquet present" gate the shared driver applies is our
    # signal the hour was touched by the recompute pipeline at least once;
    # skipping hours without it avoids materializing rollups for ingest gaps.
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=SLOW_URLS_BUNDLE_FILENAME,
        label="slow_urls",
        builder=build_slow_urls_bundles,
        logger=logger,
    )


def build_slow_urls_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a top-K-by-p95 URLs rollup for each closed hour in ``hours``.

    Skips the active UTC hour (still being written; live SQL serves it
    when the reader's window includes the active hour). Skips services
    whose schema lacks ``url`` (the panel has no input). Skips a service
    whose schema lacks both ``ottfb`` and ``ttfb`` (no latency to rank
    on).

    Idempotent — atomic tmp + rename. Returns the number of bundles
    written this call.
    """

    def eligibility(cols, table_ident):
        if "url" not in cols:
            # No URL column → no rows to rank. Don't write empty files;
            # the reader's "missing closed hour" check will fall back to
            # raw, which itself returns has_data=False for this service.
            return None
        # Latency expression from the canonical catalog; absent=None →
        # no latency column, no percentile to rank on.
        return metric_expressions.origin_latency_us(cols, absent=None)

    def build_copy_sql(lat_us_expr, table_ident, start_iso, end_iso, tmp_path):
        lat_sum = metric_expressions.sum_or_zero("lat_us", zero="0.0")
        return (
            f"COPY ("
            f"  SELECT "
            f"    url, "
            f"    CAST(COUNT(*) AS BIGINT) AS requests, "
            f"    CAST(COUNT(*) FILTER (WHERE lat_us IS NOT NULL) AS BIGINT) AS lat_us_count, "
            f"    CAST({lat_sum} AS DOUBLE) AS lat_us_sum, "
            f"    CAST(MEDIAN(lat_us) AS DOUBLE) AS p50_us, "
            f"    CAST(APPROX_QUANTILE(lat_us, 0.95) AS DOUBLE) AS p95_us, "
            f"    CAST(APPROX_QUANTILE(lat_us, 0.99) AS DOUBLE) AS p99_us "
            f"  FROM ("
            f"    SELECT url, {lat_us_expr} AS lat_us "
            f"    FROM {table_ident} "
            f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"      AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"      AND url IS NOT NULL"
            f"  ) "
            f"  WHERE lat_us IS NOT NULL "
            f"  GROUP BY url "
            f"  HAVING COUNT(*) >= {SLOW_URLS_BUNDLE_MIN_REQUESTS_PER_HOUR} "
            f"  ORDER BY APPROX_QUANTILE(lat_us, 0.95) DESC "
            f"  LIMIT {SLOW_URLS_BUNDLE_TOP_K}"
            f") {parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=SLOW_URLS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_su_",
        label="slow_urls",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def _make_perf_latency_copy_sql(col: str, min_req: int):
    def build_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY ("
            f"  SELECT value, "
            f"    CAST(COUNT(*) AS BIGINT) AS requests, "
            f"    CAST(COUNT(*) FILTER (WHERE elapsed IS NOT NULL) AS BIGINT) AS elapsed_count, "
            f"    CAST(COALESCE(SUM(elapsed), 0.0) AS DOUBLE) AS elapsed_sum, "
            f"    CAST(MEDIAN(elapsed) AS DOUBLE) AS p50_us, "
            f"    CAST(APPROX_QUANTILE(elapsed, 0.95) AS DOUBLE) AS p95_us, "
            f"    CAST(APPROX_QUANTILE(elapsed, 0.99) AS DOUBLE) AS p99_us "
            f"  FROM ("
            f'    SELECT CAST("{col}" AS VARCHAR) AS value, CAST(elapsed AS DOUBLE) AS elapsed '
            f"    FROM {table_ident} "
            f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"      AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f'      AND "{col}" IS NOT NULL'
            f"  ) "
            f"  WHERE elapsed IS NOT NULL "
            f"  GROUP BY value "
            f"  HAVING COUNT(*) >= {min_req} "
            f"  ORDER BY APPROX_QUANTILE(elapsed, 0.99) DESC "
            f"  LIMIT {PERF_LATENCY_BUNDLE_TOP_K}"
            f") {parquet_zstd_target(tmp_path)}"
        )

    return build_copy_sql


_PERF_DIMS: tuple[MultiBundleSpec, ...] = (
    (
        "url",
        PERF_TOP_URLS_BUNDLE_FILENAME,
        ("elapsed", "url"),
        _make_perf_latency_copy_sql("url", PERF_URLS_MIN_REQUESTS_PER_HOUR),
    ),
    (
        "asn",
        PERF_TOP_ASNS_BUNDLE_FILENAME,
        ("elapsed", "asn"),
        _make_perf_latency_copy_sql("asn", PERF_ASNS_MIN_REQUESTS_PER_HOUR),
    ),
)


def build_perf_latency_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the perf_top_urls + perf_top_asns rollups for each closed hour."""
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_PERF_DIMS,
        group_label="perf_latency",
        tmp_prefix=".tmp_pl_",
        logger=logger,
        driver=build_per_hour_bundles,
    )


def backfill_perf_latency_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build perf_latency rollups for every closed hour that
    has all_fields.parquet but no perf_top_urls.parquet yet.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=PERF_TOP_URLS_BUNDLE_FILENAME,
        label="perf_latency",
        builder=build_perf_latency_bundles,
        logger=logger,
    )


def _build_ttl_dist_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    return (
        f"COPY ("
        f"  SELECT "
        f"    CASE "
        f"        WHEN ttl <= 0 THEN '0s' "
        f"        WHEN ttl <= 10 THEN '<10s' "
        f"        WHEN ttl <= 30 THEN '<30s' "
        f"        WHEN ttl <= 60 THEN '<1m' "
        f"        WHEN ttl <= 300 THEN '<5m' "
        f"        WHEN ttl <= 600 THEN '<10m' "
        f"        WHEN ttl <= 1800 THEN '<30m' "
        f"        WHEN ttl <= 3600 THEN '<1h' "
        f"        WHEN ttl <= 10800 THEN '<3h' "
        f"        WHEN ttl <= 21600 THEN '<6h' "
        f"        WHEN ttl <= 43200 THEN '<12h' "
        f"        WHEN ttl <= 86400 THEN '<1d' "
        f"        WHEN ttl <= 259200 THEN '<3d' "
        f"        WHEN ttl <= 604800 THEN '<1w' "
        f"        WHEN ttl <= 1209600 THEN '<2w' "
        f"        WHEN ttl <= 2592000 THEN '<30d' "
        f"        WHEN ttl <= 7776000 THEN '<90d' "
        f"        WHEN ttl <= 31536000 THEN '<1y' "
        f"        ELSE '>1y' "
        f"    END AS bucket, "
        f"    CAST(COUNT(*) AS BIGINT) AS count, "
        f"    CAST(MIN(ttl) AS BIGINT) AS min_ttl "
        f"  FROM {table_ident} "
        f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
        f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
        f"    AND ttl IS NOT NULL "
        f"  GROUP BY 1"
        f") {parquet_zstd_target(tmp_path)}"
    )


_PERF_TTL_DIMS: tuple[MultiBundleSpec, ...] = (
    ("ttl_dist", PERF_TTL_DIST_BUNDLE_FILENAME, ("ttl",), _build_ttl_dist_copy_sql),
)


def build_perf_dims_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the perf_ttl_dist rollup for each closed hour in ``hours``."""
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_PERF_TTL_DIMS,
        group_label="perf_dims",
        tmp_prefix=".tmp_pd_",
        logger=logger,
        driver=build_per_hour_bundles,
    )


def backfill_perf_dims_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build the perf_dims rollup for every closed hour that
    has ``all_fields.parquet`` but is missing the perf_ttl_dist file."""
    return backfill_multi_spec_bundles(
        service_id,
        source,
        specs=_PERF_TTL_DIMS,
        group_label="perf_dims",
        tmp_prefix=".tmp_pd_",
        logger=logger,
        driver=build_per_hour_bundles,
    )


# ── Re-exports from network_dims for backwards compatibility ─────────────────

from . import network_dims  # noqa: E402
from .network_dims import (  # noqa: E402, F401
    _DIMS,
    _asn_sql,
    _build_sql,
    _country_sql,
    _pop_sql,
    _region_sql,
    backfill_network_geo_bundles,
    backfill_network_heatmap_bundles,
    backfill_network_quality_bundles,
    backfill_network_rtt_bundles,
    backfill_network_speed_bundles,
    backfill_network_summary_bundles,
    backfill_pop_health_bundles,
    build_network_geo_bundles,
    build_network_heatmap_bundles,
    build_network_quality_bundles,
    build_network_rtt_bundles,
    build_network_speed_bundles,
    build_network_summary_bundles,
    build_pop_health_bundles,
)

install_shim_module(__name__, (network_dims,))
