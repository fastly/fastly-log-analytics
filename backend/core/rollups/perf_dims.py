"""Per-hour performance-dimension rollup feeding /api/performance/aggregates'
``ttl_dist`` histogram panel.

The panel is an all-rows live scan of the filtered TEMP table today (the
``ttl`` histogram CASE in ``backend/repositories/performance.py``). Pre-
aggregating each closed hour to its bucket counts removes the all-rows scan
from the catalog temp, so the temp can shrink to nothing — part of the
"route wide-window sections to rollups so the shared temp_table_create can be
skipped" work already shipped for origin + security.

Unlike the percentile rollups (slow_urls / origin_dims) the math here is EXACT
across hours — ``count`` SUMs and ``min_ttl`` is MIN-of-MIN (composing the
live query's ``ORDER BY min_ttl``). The reader carries NO ``_approx`` flag.

This module emits ONE file per closed hour (mirrors :mod:`.security_dims`'s
multi-bundle scaffold with a single entry):

  ``rollups/hour_bundled/hour=H/perf_ttl_dist.parquet``   (ttl)

Schema:

perf_ttl_dist.parquet (the ``ttl_dist`` histogram in performance.py):
  bucket   VARCHAR  -- the ttl_dist CASE label (byte-for-byte)
  count    BIGINT   -- COUNT(*) for this bucket in this hour
  min_ttl  BIGINT   -- MIN(ttl) for cross-hour bucket ordering

The bundle gates on its required column (``ttl``); a service missing the
column skips the bundle. The window-predicate + ``table_ident`` substitution
match :mod:`.security_dims` (writers must NOT import ``repositories``;
import-linter enforces this — the CASE/WHERE body is replicated verbatim).

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.security_dims`.
"""

from __future__ import annotations

import logging

from ._common import (
    PERF_TTL_DIST_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def _build_ttl_dist_copy_sql(ctx: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    # ttl_dist: histogram of ttl over the fixed time buckets. CASE replicated
    # byte-for-byte from the ttl_dist section in performance.py so the reader's
    # SUM(count) per bucket equals the live scan's count per bucket, and the
    # cross-hour MIN(min_ttl) composes the live ``ORDER BY min_ttl``.
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
        f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


# (label, output filename, required column(s), build_copy_sql). One COPY.
# The required column gates whether a service can produce the bundle — a
# service with no ``ttl`` column skips it.
_PERF_DIMS = (("ttl_dist", PERF_TTL_DIST_BUNDLE_FILENAME, ("ttl",), _build_ttl_dist_copy_sql),)


def build_perf_dims_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write the perf_ttl_dist rollup for each closed hour in ``hours``.

    Skips:
      - The active UTC hour (still being written).
      - The bundle when the required ``ttl`` column is absent.

    Idempotent — atomic tmp+rename per file under the per-service iceberg lock.
    Returns the number of parquet files written this call. The bundle drives the
    shared :func:`build_per_hour_bundles` once; the per-bundle eligibility
    callback skips the service when the needed column is absent.
    """
    written = 0
    for label, filename, req_cols, build_copy_sql in _PERF_DIMS:

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
            tmp_prefix=".tmp_pd_",
            label=f"perf_dims({label})",
            describe_label="perf_dims",
            eligibility=eligibility,
            build_copy_sql=build_copy_sql,
            logger=logger,
        )
    return written


def backfill_perf_dims_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build the perf_dims rollup for every closed hour that
    has ``all_fields.parquet`` but is missing the perf_ttl_dist file.

    Walks per bundle (one :func:`backfill_missing_bundles` walk per filename),
    mirroring :func:`backend.core.rollups.security_dims.backfill_security_dims_bundles`
    so a future multi-bundle expansion self-heals each bundle independently.
    Idempotent. Returns files written.
    """
    written = 0
    for label, filename, req_cols, build_copy_sql in _PERF_DIMS:

        def _one_bundle_builder(
            sid: str,
            src: dict,
            hours: list[str],
            _filename: str = filename,
            _label: str = label,
            _req_cols: tuple[str, ...] = req_cols,
            _build_copy_sql=build_copy_sql,
        ) -> int:
            def eligibility(cols, table_ident, req_cols=_req_cols):
                for c in req_cols:
                    if c not in cols:
                        return None
                return True

            return build_per_hour_bundles(
                sid,
                src,
                hours,
                bundle_filename=_filename,
                tmp_prefix=".tmp_pd_",
                label=f"perf_dims({_label})",
                describe_label="perf_dims",
                eligibility=eligibility,
                build_copy_sql=_build_copy_sql,
                logger=logger,
            )

        written += backfill_missing_bundles(
            service_id,
            source,
            bundle_filename=filename,
            label=f"perf_dims({label})",
            builder=_one_bundle_builder,
            logger=logger,
        )
    return written
