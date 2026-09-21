"""Precomputed RUM rollups and aggregates writer for standard mode (DuckDB)."""

from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def table_exists(con, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False


def recompute_rum_aggregates(con, service_id: str, hours: list[datetime] | list[str] | None = None) -> None:
    """Ensure RUM aggregate tables exist, find active hours, and update them idempotently."""
    logger.info("[rum_rollups] Ensuring RUM aggregate schemas exist...")
    con.execute("""
        CREATE TABLE IF NOT EXISTS rum_vitals_aggregates (
            service_id VARCHAR,
            bucket_start TIMESTAMPTZ,
            dimension VARCHAR,
            value VARCHAR,
            event_count BIGINT,
            value_sum DOUBLE,
            good_count BIGINT,
            ni_count BIGINT,
            poor_count BIGINT,
            p50_value DOUBLE,
            p75_value DOUBLE,
            p99_value DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS rum_error_aggregates (
            service_id VARCHAR,
            bucket_start TIMESTAMPTZ,
            dimension VARCHAR,
            value VARCHAR,
            error_count BIGINT
        )
    """)

    has_vitals = table_exists(con, "client_vitals")
    has_errors = table_exists(con, "client_errors")

    if not has_vitals and not has_errors:
        logger.info("[rum_rollups] %s: No client_vitals or client_errors table found. Skipping.", service_id)
        return

    # 2. Identify the hours that have raw data
    hours_set = set()
    if hours is not None:
        for h in hours:
            if isinstance(h, str):
                try:
                    parsed_dt = datetime.fromisoformat(h.replace("Z", "+00:00")).replace(
                        minute=0, second=0, microsecond=0, tzinfo=None
                    )
                    hours_set.add(parsed_dt)
                except ValueError:
                    pass
            elif isinstance(h, datetime):
                # Ensure we work with naive datetimes or normalize timezone
                naive_dt = h.replace(tzinfo=None).replace(minute=0, second=0, microsecond=0)
                hours_set.add(naive_dt)
    else:
        if has_vitals:
            try:
                res = con.execute(
                    "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_vitals WHERE timestamp IS NOT NULL"
                ).fetchall()
                for r in res:
                    if r[0]:
                        # Normalize timezone to match the other path
                        dt_val = r[0]
                        if hasattr(dt_val, "replace"):
                            dt_val = dt_val.replace(tzinfo=None)
                        hours_set.add(dt_val)
            except Exception as e:
                logger.warning("[rum_rollups] Failed to query active hours from client_vitals: %s", e)

        if has_errors:
            try:
                res = con.execute(
                    "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_errors WHERE timestamp IS NOT NULL"
                ).fetchall()
                for r in res:
                    if r[0]:
                        dt_val = r[0]
                        if hasattr(dt_val, "replace"):
                            dt_val = dt_val.replace(tzinfo=None)
                        hours_set.add(dt_val)
            except Exception as e:
                logger.warning("[rum_rollups] Failed to query active hours from client_errors: %s", e)

    if not hours_set:
        logger.info("[rum_rollups] %s: No active RUM data hours found. Skipping.", service_id)
        return

    # 3. Format hours as SQL TIMESTAMPTZ list
    hours_list = sorted(list(hours_set))
    hours_str = ", ".join(f"TIMESTAMPTZ '{h.isoformat()}'" for h in hours_list)

    logger.info(
        "[rum_rollups] %s: Recomputing aggregates for %d hours: %s",
        service_id,
        len(hours_list),
        [h.isoformat() for h in hours_list],
    )

    # 4. Delete existing aggregates for these hours
    con.execute(
        f"DELETE FROM rum_vitals_aggregates WHERE service_id = ? AND bucket_start IN ({hours_str})", [service_id]
    )
    con.execute(
        f"DELETE FROM rum_error_aggregates WHERE service_id = ? AND bucket_start IN ({hours_str})", [service_id]
    )

    distinct_id = "hash(COALESCE(NULLIF(req_id, ''), concat(cid, '_', CAST(epoch(timestamp) AS BIGINT))))"

    # 5. Populate vitals aggregates
    if has_vitals:
        con.execute(
            f"""
            INSERT INTO rum_vitals_aggregates
            -- Dimension: total
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'total' AS dimension,
                CASE WHEN metric_name LIKE 'event_%' THEN 'interactions' ELSE 'pageviews' END AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, value

            UNION ALL

            -- Dimension: metric_name
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'metric_name' AS dimension,
                metric_name AS value,
                COUNT(*) AS event_count,
                CAST(COALESCE(SUM(metric_value), 0.0) AS DOUBLE) AS value_sum,
                COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count,
                COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count,
                COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count,
                CAST(MEDIAN(metric_value) AS DOUBLE) AS p50_value,
                CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
                CAST(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, metric_name

            UNION ALL

            -- Dimension: browser
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'browser' AS dimension,
                COALESCE(browser, 'Unknown') AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, browser

            UNION ALL

            -- Dimension: os
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'os' AS dimension,
                COALESCE(os, 'Unknown') AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, os

            UNION ALL

            -- Dimension: device
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'device' AS dimension,
                COALESCE(device, 'Unknown') AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, device

            UNION ALL

            -- Dimension: pathname
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'pathname' AS dimension,
                pathname AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}) AND pathname IS NOT NULL
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: path_metric_load
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'path_metric_load' AS dimension,
                pathname AS value,
                COUNT(*) AS event_count,
                CAST(COALESCE(SUM(metric_value), 0.0) AS DOUBLE) AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
              AND pathname IS NOT NULL
              AND metric_name IN ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: path_metric_lcp
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'path_metric_lcp' AS dimension,
                pathname AS value,
                COUNT(*) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
              AND pathname IS NOT NULL
              AND metric_name IN ('LCP', 'lcp')
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: path_metric_cls
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'path_metric_cls' AS dimension,
                pathname AS value,
                COUNT(*) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
              AND pathname IS NOT NULL
              AND metric_name IN ('CLS', 'cls')
            GROUP BY bucket_start, pathname
        """,
            [service_id] * 9,
        )

    # 6. Populate error aggregates
    if has_errors:
        con.execute(
            f"""
            INSERT INTO rum_error_aggregates
            -- Dimension: total
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'total' AS dimension,
                'errors' AS value,
                COUNT(DISTINCT {distinct_id}) AS error_count
            FROM client_errors
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start

            UNION ALL

            -- Dimension: pathname
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'pathname' AS dimension,
                pathname AS value,
                COUNT(DISTINCT {distinct_id}) AS error_count
            FROM client_errors
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}) AND pathname IS NOT NULL
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: exception
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'exception' AS dimension,
                concat(COALESCE(error_message, ''), '|', COALESCE(error_file, ''), '|', CAST(COALESCE(error_line, 0) AS VARCHAR), '|', CAST(COALESCE(error_col, 0) AS VARCHAR)) AS value,
                COUNT(*) AS error_count
            FROM client_errors
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, value
        """,
            [service_id] * 3,
        )

    logger.info("[rum_rollups] %s: RUM aggregates successfully updated for %d hours", service_id, len(hours_list))
