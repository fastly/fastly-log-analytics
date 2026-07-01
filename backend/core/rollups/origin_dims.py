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

from ._common import (
    ORIGIN_DIMS_BUNDLE_TOP_K,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_IP_MIN_REQUESTS_PER_HOUR,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def _lat_us_expr(cols: set[str]) -> str | None:
    """Origin latency-us expression, matching origin_latency_us_expr in
    ``backend.repositories._base`` (kept in sync rather than imported to
    dodge the import cycle). Returns ``None`` when no latency column exists.
    """
    if "ottfb" in cols and "ttfb" in cols:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    if "ottfb" in cols:
        return '"ottfb"'
    if "ttfb" in cols:
        return '"ttfb" * 1000000.0'
    return None


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
        f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
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
        f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
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
        f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


# (output filename, required key column, build_copy_sql). One COPY each. The
# latency column is required for all three; the key column gates which bundle
# a service can produce (e.g. no ``pop`` → skip the pop bundle but still write
# ip/path). The ``ost`` requirement for ip is enforced inside its eligibility.
_ORIGIN_DIMS = (
    ("pop", ORIGIN_POP_BUNDLE_FILENAME, _build_pop_copy_sql, ()),
    ("oip", ORIGIN_IP_BUNDLE_FILENAME, _build_ip_copy_sql, ("ost",)),
    ("edge", ORIGIN_PATH_BUNDLE_FILENAME, _build_path_copy_sql, ()),
)


def build_origin_dims_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the origin_pop + origin_ip + origin_path rollups for each closed
    hour in ``hours``.

    Skips:
      - The active UTC hour (still being written).
      - Services whose schema lacks any latency column (ottfb/ttfb) — no
        percentile to compute, so none of the three bundles are written.
      - A bundle whose required key column is absent (e.g. no ``oip`` or no
        ``ost`` → skip origin_ip but still write origin_pop / origin_path when
        their columns exist).

    Idempotent — atomic tmp+rename per file under the per-service iceberg lock.
    Returns the number of parquet files written this call (a fully built hour
    with all three bundles counts as 3). Each bundle drives the shared
    :func:`build_per_hour_bundles` once; the per-bundle eligibility callback
    skips the service for that bundle when a needed column is absent.
    """
    written = 0
    for key_col, filename, build_copy_sql, extra_cols in _ORIGIN_DIMS:

        def eligibility(cols, table_ident, key_col=key_col, extra_cols=extra_cols):
            lat_us_expr = _lat_us_expr(set(cols))
            if lat_us_expr is None:
                return None
            if key_col not in cols:
                return None
            for c in extra_cols:
                if c not in cols:
                    return None
            return lat_us_expr

        written += build_per_hour_bundles(
            service_id,
            source,
            hours,
            bundle_filename=filename,
            tmp_prefix=".tmp_od_",
            label=f"origin_dims({key_col})",
            describe_label="origin_dims",
            eligibility=eligibility,
            build_copy_sql=build_copy_sql,
            logger=logger,
        )
    return written


def backfill_origin_dims_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build the origin_dims rollups for every closed hour that
    has ``all_fields.parquet`` but is missing one of the origin_dims files.

    Mirrors :func:`backfill_perf_latency_bundles`. Uses the pop file as the
    sentinel (the dominant panel); ``build_origin_dims_bundles`` writes all
    three present bundles when their columns exist. Idempotent. Returns files
    written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=ORIGIN_POP_BUNDLE_FILENAME,
        label="origin_dims",
        builder=build_origin_dims_bundles,
        logger=logger,
    )
