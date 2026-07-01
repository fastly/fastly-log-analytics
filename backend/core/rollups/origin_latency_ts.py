"""Per-hour MINUTE-granular origin-latency-percentile time-series rollup
feeding /api/origin/aggregates' ``timeseries`` panel.

This is a NEW hybrid shape — no existing rollup combined a time axis with
percentiles. It fuses two templates:

  * the minute-granular time-series WRITER shape of :mod:`.verified_bots_ts`
    (``date_trunc('minute', timestamp) AS bucket_ts``; the reader re-buckets
    via ``time_bucket`` to any whole-minute width), and
  * the request-weighted percentile merge math of :mod:`.slow_urls`
    (``SUM(p_us * cnt) / NULLIF(SUM(cnt), 0)`` across minutes → biased,
    ``_approx``).

The live panel (:data:`backend.repositories._sql.origin.TIMESERIES_BUCKETED`)
buckets the filtered temp table per ``time_bucket(interval, timestamp)`` into
``COUNT(*) AS miss_count`` + ``MEDIAN/APPROX_QUANTILE(lat) AS value`` (ms),
where ``lat IS NOT NULL``. The page sends ``split_by_leg=false``, so this
writer aggregates over edge (NON-split only).

Each closed hour stores BOTH latency bases so the reader can serve either
metric (the page toggles ttfb / ttlb) without two writers:

  bucket_ts     TIMESTAMPTZ  -- minute-truncated UTC instant
  ttfb_count    BIGINT       -- COUNT(*) FILTER (ttfb_lat IS NOT NULL)  == miss_count for ttfb
  ttfb_p50_us   DOUBLE       -- MEDIAN(ttfb_lat) within (minute)
  ttfb_p95_us   DOUBLE       -- APPROX_QUANTILE(ttfb_lat, 0.95)
  ttfb_p99_us   DOUBLE       -- APPROX_QUANTILE(ttfb_lat, 0.99)
  ttlb_count    BIGINT       -- COUNT(*) FILTER (ttlb_lat IS NOT NULL)
  ttlb_p50_us   DOUBLE
  ttlb_p95_us   DOUBLE
  ttlb_p99_us   DOUBLE

``ttfb_lat = COALESCE("ottfb", "ttfb"*1e6)`` (or "ottfb" / "ttfb"*1e6 by
availability — the precomputed ``lat_us``); ``ttlb_lat = "ottlb"``. When a
latency column is absent the corresponding columns are written as a constant
``count=0`` + ``NULL`` percentiles, keeping the parquet SCHEMA UNIFORM across
services/hours so the reader never hits a BinderException on a missing column
(same uniform-shape posture as :mod:`.time_series`).

Eligibility: skip the service ENTIRELY when there is NO ttfb-latency column
(no ``ottfb`` and no ``ttfb``) — there is nothing to roll up. When ``ottlb``
is absent the ttlb_* columns are the constant 0 / NULL block.

``lat`` expressions are replicated verbatim here rather than imported from
``backend.repositories._base`` to avoid the import-graph cycle (rollups must
NOT import ``repositories``; import-linter enforces this).

Output: ``rollups/hour_bundled/hour=H/origin_latency_ts.parquet``.

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.verified_bots_ts`. The day compactor
(:func:`backend.core.rollups.day_bundles.compact_origin_latency_ts_closed_days_to_daily`)
PRESERVES the minute dimension (this is a time series, not a leaderboard).
"""

from __future__ import annotations

import logging
from typing import cast

from ._common import (
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def _ttfb_lat_expr(cols: set[str]) -> str | None:
    """ttfb latency-us expression, matching origin_latency_us_expr in
    ``backend.repositories._base`` (kept in sync rather than imported to dodge
    the import cycle). Returns ``None`` when no ttfb-latency column exists.
    """
    if "ottfb" in cols and "ttfb" in cols:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    if "ottfb" in cols:
        return '"ottfb"'
    if "ttfb" in cols:
        return '"ttfb" * 1000000.0'
    return None


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

    Idempotent — atomic tmp+rename under the per-service iceberg lock.
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
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
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
