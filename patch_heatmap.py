import re

with open("backend/repositories/_base.py") as f:
    content = f.read()

heatmap_search = r"""        sql = \(
            f"SELECT"
            f"  asn,"
            f"  hour_ts                                                             AS bucket_ts,"
            f"  resp_bytes_sum / 3600.0                                            AS throughput_bps,"
            f"  rtt_p50_us                                                         AS rtt_med_us,"
            f"  rtt_min_p50_us                                                     AS rtt_baseline_us,"
            f"  CAST\(rtt_p50_us AS BIGINT\) - CAST\(COALESCE\(rtt_min_p50_us, rtt_p50_us\) AS BIGINT\) AS rtt_congestion_us,"
            f"  ploss_sum / NULLIF\(ploss_count, 0\)                                AS avg_ploss,"
            f"  rtt_var_p50_us                                                     AS jitter_us,"
            f"  errors \* 100.0 / NULLIF\(reqs, 0\)                                  AS error_pct,"
            f"  CAST\(reqs AS BIGINT\)                                               AS reqs"
            f" FROM read_parquet\(\[\{paths_sql\}\]\)"
            f" WHERE hour_ts >= TIMESTAMPTZ '\{st_iso\}' AND hour_ts < TIMESTAMPTZ '\{et_iso\}'"
            f" ORDER BY reqs DESC"
        \)"""

heatmap_replace = r"""        from datetime import UTC, datetime
        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for
        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()

            # The active hour duration in seconds for throughput calculation
            active_duration_secs = max(1, (et - active_hour_start).total_seconds())

            sql = (
                f"SELECT * FROM ("
                f"  SELECT"
                f"    asn,"
                f"    hour_ts                                                             AS bucket_ts,"
                f"    resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"    rtt_p50_us                                                         AS rtt_med_us,"
                f"    rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"    CAST(rtt_p50_us AS BIGINT) - CAST(COALESCE(rtt_min_p50_us, rtt_p50_us) AS BIGINT) AS rtt_congestion_us,"
                f"    ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
                f"    rtt_var_p50_us                                                     AS jitter_us,"
                f"    errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
                f"    CAST(reqs AS BIGINT)                                               AS reqs"
                f"   FROM read_parquet([{paths_sql}])"
                f"   WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"   UNION ALL "
                f"   SELECT"
                f"    asn,"
                f"    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
                f"    CAST(SUM(resp_bytes) AS DOUBLE) / {active_duration_secs} AS throughput_bps,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_baseline_us," # No min baseline for active hour
                f"    0::BIGINT AS rtt_congestion_us,"
                f"    0::DOUBLE AS avg_ploss," # Ploss requires packet info, assume 0 for live
                f"    0::DOUBLE AS jitter_us,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs"
                f"   FROM {base_table} "
                f"   WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
                f"     AND asn IS NOT NULL"
                f"   GROUP BY asn"
                f")"
                f" ORDER BY reqs DESC"
            )
        else:
            sql = (
                f"SELECT"
                f"  asn,"
                f"  hour_ts                                                             AS bucket_ts,"
                f"  resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"  rtt_p50_us                                                         AS rtt_med_us,"
                f"  rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"  CAST(rtt_p50_us AS BIGINT) - CAST(COALESCE(rtt_min_p50_us, rtt_p50_us) AS BIGINT) AS rtt_congestion_us,"
                f"  ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
                f"  rtt_var_p50_us                                                     AS jitter_us,"
                f"  errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
                f"  CAST(reqs AS BIGINT)                                               AS reqs"
                f" FROM read_parquet([{paths_sql}])"
                f" WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f" ORDER BY reqs DESC"
            )"""

content = re.sub(heatmap_search, heatmap_replace, content)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
