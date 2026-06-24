"""Per-hour top-N URLs rollup writer feeding the /origin slow_urls panel.

The slow_urls panel computes top URLs by p95 origin latency. Done live
over a 30-day window on the active service it scans 5.8 M raw rows and
GROUP-BYs over 111 K distinct URLs to compute MEDIAN + APPROX_QUANTILE
× 3, taking ~5.6 s of pool time per /api/origin/aggregates call (see the
perf audit). This writer pre-aggregates each closed hour to its top-K
URLs by p95, with the exact per-hour p50/p95/p99 + request count baked
in. The reader (:meth:`QueryRunner.try_slow_urls_from_rollup`) then
request-weight-averages across hours instead of re-scanning raw rows.

Output: ``rollups/hour_bundled/hour=H/slow_urls.parquet`` with K=100 rows
per hour, schema:

  url             VARCHAR
  requests        BIGINT   -- COUNT(*) for this URL in this hour
  lat_us_count    BIGINT   -- rows with lat_us NOT NULL
  lat_us_sum      DOUBLE   -- SUM(lat_us) for mean reconstruction
  p50_us          DOUBLE   -- MEDIAN(lat_us) exact within this hour
  p95_us          DOUBLE   -- APPROX_QUANTILE(lat_us, 0.95) within hour
  p99_us          DOUBLE   -- APPROX_QUANTILE(lat_us, 0.99) within hour

Eligibility (URLs included in the top-K):
  * url IS NOT NULL
  * COUNT(*) >= SLOW_URLS_BUNDLE_MIN_REQUESTS_PER_HOUR (default 5)
  * ranked by p95_us DESC, capped at SLOW_URLS_BUNDLE_TOP_K (default 100)

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.time_series`.

Note on percentile-combine across hours: DuckDB doesn't ship sketch-
combine (see ``_base.py:1387``). The reader returns a request-weighted
average of per-hour p95 (and p50/p99) — biased but the ranking is
preserved across the URLs that dominate the panel. The response surfaces
``{_approx: true}`` so the UI can show a "30 d approximate" note.
"""

from __future__ import annotations

import logging

from ._common import (
    SLOW_URLS_BUNDLE_FILENAME,
    SLOW_URLS_BUNDLE_MIN_REQUESTS_PER_HOUR,
    SLOW_URLS_BUNDLE_TOP_K,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


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
        # Build the latency expression matching backend.repositories._base
        # .origin_latency_us_expr — kept in sync rather than imported to
        # avoid the import-graph cycle (repositories depends on core).
        if "ottfb" in cols and "ttfb" in cols:
            return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
        elif "ottfb" in cols:
            return '"ottfb"'
        elif "ttfb" in cols:
            return '"ttfb" * 1000000.0'
        # No latency columns at all → no percentile to rank on.
        return None

    def build_copy_sql(lat_us_expr, table_ident, start_iso, end_iso, tmp_path):
        return (
            f"COPY ("
            f"  SELECT "
            f"    url, "
            f"    CAST(COUNT(*) AS BIGINT) AS requests, "
            f"    CAST(COUNT(*) FILTER (WHERE lat_us IS NOT NULL) AS BIGINT) AS lat_us_count, "
            f"    CAST(COALESCE(SUM(lat_us), 0.0) AS DOUBLE) AS lat_us_sum, "
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
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
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
