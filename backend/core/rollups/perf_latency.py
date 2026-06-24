"""Per-hour top-N url/asn latency rollup feeding /api/performance/aggregates'
``top_urls`` + ``top_asns`` panels.

Both panels are 2-pass queries: take the top url/asn by request count, then
compute ``avg + p50 + p95 + p99`` of ``elapsed`` (µs) over the window, sorted
by a caller ``sort_by`` (default p99). On prod 30 d that's ~1.45 s (urls) +
~0.77 s (asns) of pool time per /api/performance/aggregates call.

This is the SAME per-dimension-percentiles-over-window shape as
:mod:`.slow_urls` (per-URL origin latency) and :mod:`.network_rtt` (per-ASN
RTT) — there is NO time-bucket axis — so it rolls up the same way: each closed
hour pre-aggregates to its top-K by p99 with the exact per-hour
p50/p95/p99 + request count + (sum, count) for mean reconstruction. The reader
(:meth:`QueryRunner.try_perf_latency_from_rollup`) request-weight-averages the
percentiles across hours and re-ranks by the caller's ``sort_by``.

One module covers BOTH dimensions (they differ only in the GROUP-BY column +
the min-requests floor), writing two files per closed hour:

  ``rollups/hour_bundled/hour=H/perf_top_urls.parquet``  (top-K by p99, url)
  ``rollups/hour_bundled/hour=H/perf_top_asns.parquet``  (top-K by p99, asn)

Schema (both files):

  value           VARCHAR  -- the url / asn (asn cast to text)
  requests        BIGINT   -- COUNT(*) for this value in this hour
  elapsed_count   BIGINT   -- rows with elapsed NOT NULL
  elapsed_sum     DOUBLE   -- SUM(elapsed) for mean reconstruction
  p50_us          DOUBLE   -- MEDIAN(elapsed) exact within this hour
  p95_us          DOUBLE   -- APPROX_QUANTILE(elapsed, 0.95) within hour
  p99_us          DOUBLE   -- APPROX_QUANTILE(elapsed, 0.99) within hour

Percentile-combine across hours is request-weighted-average (DuckDB ships no
sketch-combine; see ``_base.py:1387``) — biased but the ranking is preserved
for the slow values that dominate the panel; the response carries ``_approx``.

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.slow_urls`.
"""

from __future__ import annotations

import logging

from ._common import (
    PERF_ASNS_MIN_REQUESTS_PER_HOUR,
    PERF_LATENCY_BUNDLE_TOP_K,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_URLS_MIN_REQUESTS_PER_HOUR,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)

# (dimension column, output filename, per-hour min requests). One COPY each.
_PERF_DIMS = (
    ("url", PERF_TOP_URLS_BUNDLE_FILENAME, PERF_URLS_MIN_REQUESTS_PER_HOUR),
    ("asn", PERF_TOP_ASNS_BUNDLE_FILENAME, PERF_ASNS_MIN_REQUESTS_PER_HOUR),
)


def build_perf_latency_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the perf_top_urls + perf_top_asns rollups for each closed hour.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``elapsed`` (no latency to rank on)
      - A dimension whose column is absent (e.g. no ``asn``) — that file is
        simply not written; the other dimension still is.

    Idempotent — atomic tmp+rename per file under the per-service iceberg
    lock. Returns the number of parquet files written this call (so a fully
    built hour with both dims counts as 2).

    Unlike the single-file writers this drives the shared
    :func:`build_per_hour_bundles` once per dimension and sums the file
    counts — each dimension is an independent single-file rollup that just
    happens to share a source table. The two driver passes each describe the
    view (cheap, on a cron writer path), so ``describe_label`` keeps the
    un-suffixed ``perf_latency`` describe log while the COPY/publish warnings
    carry the per-dimension ``perf_latency(col)`` label as before.
    """
    written = 0
    for col, filename, min_req in _PERF_DIMS:

        def eligibility(cols, table_ident, col=col):
            if "elapsed" not in cols or col not in cols:
                return None
            return True

        def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path, col=col, min_req=min_req):
            # Same COPY shape as slow_urls, but on `elapsed` and grouped by
            # the dimension column; top-K cut by p99 DESC (the default sort
            # + the "slowest" intent). avg is reconstructed from
            # (elapsed_sum / elapsed_count) at read time.
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
                f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )

        written += build_per_hour_bundles(
            service_id,
            source,
            hours,
            bundle_filename=filename,
            tmp_prefix=".tmp_pl_",
            label=f"perf_latency({col})",
            describe_label="perf_latency",
            eligibility=eligibility,
            build_copy_sql=build_copy_sql,
            logger=logger,
        )
    return written


def backfill_perf_latency_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build perf_latency rollups for every closed hour that
    has all_fields.parquet but no perf_top_urls.parquet yet.

    Mirrors :func:`backfill_slow_urls_bundles`. Uses the urls file as the
    sentinel (the dominant panel); ``build_perf_latency_bundles`` writes both
    dimensions when present. Idempotent. Returns files written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=PERF_TOP_URLS_BUNDLE_FILENAME,
        label="perf_latency",
        builder=build_perf_latency_bundles,
        logger=logger,
    )
