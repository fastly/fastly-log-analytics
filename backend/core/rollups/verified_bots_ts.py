"""Per-hour minute-granular verified-bot time-series rollup feeding the
/api/security/aggregates ``verified_bots_ts`` panel.

The live panel
(:data:`backend.repositories._sql.security.VERIFIED_BOTS_TS`) re-scans the
filtered temp table on every render, doing
``unnest(string_split(waf_sig, ','))`` + a ``time_bucket`` GROUP BY to build a
category-level verified-bot time series — on prod 30 d that's ~1.2 s. This
writer pre-aggregates each closed hour to MINUTE granularity so the reader
(:meth:`QueryRunner.try_verified_bots_ts_from_rollup`) can re-bucket to any
caller-passed ``bucket_seconds`` that's a multiple of 60 with a pure integer
``SUM`` — EXACT, no approximation (verified empirically: minute-granular
re-bucketing equals direct ``time_bucket`` for every multiple-of-60 width).

Unlike :mod:`.network_speed` / :mod:`.network_rtt` there is NO top-K
leaderboard cut: the dimension is ``bot_type``, a small fixed vocabulary
(~10-30 verified-bot categories), so per hour is only ~60 min × ~20 types.

The stored ``bot_type`` and the filters mirror the live SQL byte-for-byte
(``replace(tag, 'VERIFIED-BOT.', '')``, case-insensitive pre-filter +
case-sensitive ``LIKE 'VERIFIED-BOT.%'`` cut) so the rollup result is
identical to the live query — only the time expression changes
(``date_trunc('minute', timestamp)`` instead of ``time_bucket(...)``).

Output: ``rollups/hour_bundled/hour=H/verified_bots_ts.parquet``. Schema:

  bucket_ts    TIMESTAMPTZ  -- minute-truncated UTC instant
  bot_type     VARCHAR      -- verified-bot category, prefix stripped
  count        BIGINT       -- exact request count in that (minute, bot_type)

Active-hour skip + atomic tmp+rename + per-service iceberg lock — same
convention as :mod:`.network_speed`.
"""

from __future__ import annotations

import logging

from ._common import (
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
)

logger = logging.getLogger(__name__)


def build_verified_bots_ts_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour verified_bots_ts rollup for each closed hour.

    Skips:
      - The active UTC hour (still being written)
      - Services whose schema lacks ``waf_sig`` (nothing to roll up)
      - Hours with no matching data (writes a valid empty parquet)

    Idempotent — atomic tmp+rename under the per-service iceberg lock.
    Returns the number of bundles written this call.
    """

    def eligibility(cols, table_ident):
        if "waf_sig" not in cols:
            return None
        return True

    def build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path):
        # Mirror the live VERIFIED_BOTS_TS filters/replace exactly so the
        # rollup is identical to the live query; only the time expression
        # changes (minute-truncate vs time_bucket). The case-insensitive
        # inner ILIKE is a cheap row pre-filter; the case-sensitive outer
        # LIKE is the load-bearing tag cut.
        return (
            f"COPY ("
            f"  SELECT CAST(date_trunc('minute', timestamp) AS TIMESTAMPTZ) AS bucket_ts, "
            f"         replace(tag, 'VERIFIED-BOT.', '') AS bot_type, "
            f"         CAST(COUNT(*) AS BIGINT) AS count "
            f"  FROM ("
            f"    SELECT timestamp, unnest(string_split(waf_sig, ',')) AS tag "
            f"    FROM {table_ident} "
            f"    WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"      AND timestamp <  TIMESTAMPTZ '{end_iso}' "
            f"      AND waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'"
            f"  ) sub "
            f"  WHERE tag LIKE 'VERIFIED-BOT.%' "
            f"  GROUP BY 1, 2"
            f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=VERIFIED_BOTS_TS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_vbts_",
        label="verified_bots_ts",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_verified_bots_ts_bundles(service_id: str, source: dict) -> int:
    """Self-heal pass: build verified_bots_ts.parquet for every closed hour
    that has all_fields.parquet but no verified_bots_ts.parquet yet.

    Mirrors :func:`backfill_network_speed_bundles`. Idempotent — skips
    already-built hours. Returns the number of bundles written.
    """
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=VERIFIED_BOTS_TS_BUNDLE_FILENAME,
        label="verified_bots_ts",
        builder=build_verified_bots_ts_bundles,
        logger=logger,
    )
