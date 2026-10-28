"""Per-hour overview.parquet rollup writer and backfill."""

from __future__ import annotations

import logging

from .. import metric_expressions
from ._common import (
    OVERVIEW_BUNDLE_FILENAME,
    backfill_missing_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)

_HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"


def build_overview_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write an overview rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping overview bundle", service_id, table_ident
            )
            return None

        has_cache = "cache" in cols
        select_parts: list[str] = [
            "CAST(COUNT(*) AS BIGINT) AS requests",
            *(
                (
                    f"CAST(COUNT(*) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache = 'PASS') AS BIGINT) AS pass_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache = 'SYNTH') AS BIGINT) AS synth_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache IN ('MISS', 'PASS', 'SYNTH', 'ERROR')) AS BIGINT) AS origin_requests",
                )
                if has_cache
                else (
                    "CAST(0 AS BIGINT) AS hit_requests",
                    "CAST(0 AS BIGINT) AS miss_requests",
                    "CAST(0 AS BIGINT) AS pass_requests",
                    "CAST(0 AS BIGINT) AS synth_requests",
                    "CAST(0 AS BIGINT) AS origin_requests",
                )
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('resp_bytes', where=f'cache IN {_HIT_STATES}')} AS BIGINT) AS bandwidth_saved_bytes"
                if "resp_bytes" in cols and has_cache
                else "CAST(0 AS BIGINT) AS bandwidth_saved_bytes"
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('resp_bytes')} AS BIGINT) AS total_bandwidth_bytes"
                if "resp_bytes" in cols
                else "CAST(0 AS BIGINT) AS total_bandwidth_bytes"
            ),
            *(
                (
                    f"CAST(COUNT(*) FILTER (WHERE is_shield = true AND cache IN {_HIT_STATES}) AS BIGINT) AS shield_hit_requests",
                    "CAST(COUNT(*) FILTER (WHERE is_shield = true) AS BIGINT) AS shield_total_requests",
                )
                if "is_shield" in cols and has_cache
                else ("CAST(0 AS BIGINT) AS shield_hit_requests", "CAST(0 AS BIGINT) AS shield_total_requests")
            ),
            (
                "CAST(COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS BIGINT) AS threats_blocked"
                if "waf" in cols and "waf_resp" in cols
                else "CAST(0 AS BIGINT) AS threats_blocked"
            ),
            *(
                (
                    f"CAST({metric_expressions.sum_or_zero('elapsed', zero='0.0', where=f'cache IN {_HIT_STATES}')} AS DOUBLE) AS hit_elapsed_sum",
                    f"CAST(COUNT(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_elapsed_count",
                    "CAST("
                    + metric_expressions.sum_or_zero("elapsed", zero="0.0", where="cache = 'MISS'")
                    + " AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(COUNT(elapsed) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_elapsed_count",
                )
                if "elapsed" in cols and has_cache
                else (
                    "CAST(0.0 AS DOUBLE) AS hit_elapsed_sum",
                    "CAST(0 AS BIGINT) AS hit_elapsed_count",
                    "CAST(0.0 AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(0 AS BIGINT) AS miss_elapsed_count",
                )
            ),
            *(
                (
                    f"CAST({metric_expressions.sum_or_zero('ttfb', zero='0.0', where=f'cache IN {_HIT_STATES}')} AS DOUBLE) AS hit_ttfb_sum",
                    f"CAST(COUNT(ttfb) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_ttfb_count",
                    "CAST("
                    + metric_expressions.sum_or_zero("ttfb", zero="0.0", where="cache = 'MISS'")
                    + " AS DOUBLE) AS miss_ttfb_sum",
                    "CAST(COUNT(ttfb) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_ttfb_count",
                )
                if "ttfb" in cols and has_cache
                else (
                    "CAST(0.0 AS DOUBLE) AS hit_ttfb_sum",
                    "CAST(0 AS BIGINT) AS hit_ttfb_count",
                    "CAST(0.0 AS DOUBLE) AS miss_ttfb_sum",
                    "CAST(0 AS BIGINT) AS miss_ttfb_count",
                )
            ),
            *(
                (
                    "CAST(COUNT(*) FILTER (WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL) AS BIGINT) AS waterfall_count",
                    f"CAST(COALESCE(SUM(GREATEST(0.0, (CAST(ttfb AS DOUBLE) * 1000.0) - ({'COALESCE(CAST(ottfb AS DOUBLE) / 1000.0, 0.0)' if 'ottfb' in cols else '0.0'}))) FILTER (WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL), 0.0) AS DOUBLE) AS edge_processing_sum",
                    f"CAST(COALESCE(SUM({'COALESCE(CAST(ottfb AS DOUBLE) / 1000.0, 0.0)' if 'ottfb' in cols else '0.0'}) FILTER (WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL), 0.0) AS DOUBLE) AS origin_wait_sum",
                    f"CAST(COALESCE(SUM(GREATEST(0.0, ({'COALESCE(CAST(ottlb AS DOUBLE) / 1000.0, 0.0)' if 'ottlb' in cols else '0.0'}) - ({'COALESCE(CAST(ottfb AS DOUBLE) / 1000.0, 0.0)' if 'ottfb' in cols else '0.0'}))) FILTER (WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL), 0.0) AS DOUBLE) AS origin_download_sum",
                    f"CAST(COALESCE(SUM(GREATEST(0.0, (CAST(elapsed AS DOUBLE) / 1000.0) - GREATEST({'COALESCE(CAST(ottlb AS DOUBLE) / 1000.0, 0.0)' if 'ottlb' in cols else '0.0'}, CAST(ttfb AS DOUBLE) * 1000.0))) FILTER (WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL), 0.0) AS DOUBLE) AS client_download_sum",
                )
                if "ttfb" in cols and "elapsed" in cols
                else (
                    "CAST(0 AS BIGINT) AS waterfall_count",
                    "CAST(0.0 AS DOUBLE) AS edge_processing_sum",
                    "CAST(0.0 AS DOUBLE) AS origin_wait_sum",
                    "CAST(0.0 AS DOUBLE) AS origin_download_sum",
                    "CAST(0.0 AS DOUBLE) AS client_download_sum",
                )
            ),
        ]
        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT TIMESTAMPTZ '{start_iso}' AS hour_start, {select_sql} "
            f"FROM {table_ident} WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}') "
            f"{parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=OVERVIEW_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ov_",
        label="overview",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_overview_bundles(service_id: str, source: dict) -> int:
    """Build overview.parquet for every closed hour that has all_fields.parquet but no overview.parquet."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=OVERVIEW_BUNDLE_FILENAME,
        label="overview",
        builder=build_overview_bundles,
        logger=logger,
    )


__all__ = ["_HIT_STATES", "backfill_overview_bundles", "build_overview_bundles"]
