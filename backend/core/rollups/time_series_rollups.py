"""Per-hour 1-minute time_series.parquet rollup writer."""

from __future__ import annotations

import logging

from .. import metric_expressions
from ._common import (
    TIME_SERIES_BUNDLE_FILENAME,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)


def build_time_series_bundles(
    service_id: str, source: dict, hours: list[str], *, output_root: str | None = None
) -> int:
    """Write a 1-minute time_series rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping time_series bundle", service_id, table_ident
            )
            return None

        select_parts = [
            "time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
            "CAST(COUNT(*) AS BIGINT) AS requests",
            *(
                (
                    "CAST(COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) AS BIGINT) AS status_4xx",
                    "CAST(COUNT(*) FILTER (WHERE status >= 500) AS BIGINT) AS status_5xx",
                )
                if "status" in cols
                else ("CAST(0 AS BIGINT) AS status_4xx", "CAST(0 AS BIGINT) AS status_5xx")
            ),
            *(
                (
                    "CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS hits",
                    "CAST(COUNT(*) FILTER (WHERE cache IS NOT NULL) AS BIGINT) AS cache_total",
                )
                if "cache" in cols
                else ("CAST(0 AS BIGINT) AS hits", "CAST(0 AS BIGINT) AS cache_total")
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('resp_bytes')} AS BIGINT) AS resp_bytes_sum"
                if "resp_bytes" in cols
                else "CAST(0 AS BIGINT) AS resp_bytes_sum"
            ),
            *(
                (
                    f"CAST({metric_expressions.sum_or_zero('ttfb', zero='0.0')} AS DOUBLE) AS ttfb_sum",
                    "CAST(COUNT(*) FILTER (WHERE ttfb IS NOT NULL) AS BIGINT) AS ttfb_count",
                )
                if "ttfb" in cols
                else ("CAST(0.0 AS DOUBLE) AS ttfb_sum", "CAST(0 AS BIGINT) AS ttfb_count")
            ),
        ]
        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}' GROUP BY 1) "
            f"{parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=TIME_SERIES_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ts_",
        label="time_series",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
        output_root=output_root,
    )


__all__ = ["build_time_series_bundles"]
