"""Per-hour sessions.parquet rollup writer."""

from __future__ import annotations

import logging

from .. import metric_expressions
from ._common import (
    SESSIONS_BUNDLE_FILENAME,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)


def build_session_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour ``sessions.parquet`` rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols or "ip" not in cols:
            logger.info(
                "[rollups] %s: skipping sessions bundle (timestamp=%s, ip=%s)",
                service_id,
                "timestamp" in cols,
                "ip" in cols,
            )
            return None

        ja4_expr = '"ja4"' if "ja4" in cols else "CAST(NULL AS VARCHAR)"
        select_parts = [
            "time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
            'CAST("ip" AS VARCHAR) AS ip',
            f"CAST({ja4_expr} AS VARCHAR) AS ja4",
            "MIN(timestamp) AS first_ts",
            "MAX(timestamp) AS last_ts",
            "CAST(COUNT(*) AS BIGINT) AS req_count",
            'CAST(MIN("country") AS VARCHAR) AS country' if "country" in cols else "CAST(NULL AS VARCHAR) AS country",
            'CAST(MIN("asn") AS INTEGER) AS asn' if "asn" in cols else "CAST(NULL AS INTEGER) AS asn",
            *(
                (
                    'CAST(SUM(CASE WHEN "status" BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_4xx',
                    'CAST(SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_5xx',
                )
                if "status" in cols
                else ("CAST(0 AS BIGINT) AS reqs_4xx", "CAST(0 AS BIGINT) AS reqs_5xx")
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('"resp_bytes"')} AS BIGINT) AS total_bytes"
                if "resp_bytes" in cols
                else "CAST(0 AS BIGINT) AS total_bytes"
            ),
            *(
                (
                    f"CAST({metric_expressions.sum_or_zero('"tcp_rtt"', zero='0.0')} AS DOUBLE) AS rtt_sum",
                    'CAST(COUNT(*) FILTER (WHERE "tcp_rtt" IS NOT NULL) AS BIGINT) AS rtt_count',
                )
                if "tcp_rtt" in cols
                else ("CAST(0.0 AS DOUBLE) AS rtt_sum", "CAST(0 AS BIGINT) AS rtt_count")
            ),
            *(
                (
                    'CAST(SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS BIGINT) AS edge_count',
                    'CAST(SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS BIGINT) AS shield_count',
                )
                if "edge" in cols
                else ("CAST(0 AS BIGINT) AS edge_count", "CAST(0 AS BIGINT) AS shield_count")
            ),
            'CAST(MIN("ua") AS VARCHAR) AS ua_min' if "ua" in cols else "CAST(NULL AS VARCHAR) AS ua_min",
            'CAST(MAX("edge_sid") AS VARCHAR) AS edge_sid_max'
            if "edge_sid" in cols
            else "CAST(NULL AS VARCHAR) AS edge_sid_max",
            (
                'CAST(COUNT(*) FILTER (WHERE "cmcd_sid" IS NOT NULL AND "cmcd_sid" != \'\') AS BIGINT) AS cmcd_count'
                if "cmcd_sid" in cols
                else "CAST(0 AS BIGINT) AS cmcd_count"
            ),
        ]
        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}' "
            f'AND "ip" IS NOT NULL GROUP BY 1, 2, 3) '
            f"{parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=SESSIONS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_sess_",
        label="sessions",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


__all__ = ["build_session_bundles"]
