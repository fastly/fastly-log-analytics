import re

with open("backend/repositories/_base.py") as f:
    content = f.read()

geo_search = r"""        map_sql = \(
            f"SELECT"
            f"  country,"
            f"  city,"
            f"  lat,"
            f"  lon,"
            f"  metro,"
            f"  hour_ts                                           AS bucket_ts,"
            f"  rtt_sum / NULLIF\(rtt_count, 0\)                  AS rtt_med_us,"
            f"  ploss_sum / NULLIF\(ploss_count, 0\)              AS avg_ploss,"
            f"  errors \* 100.0 / NULLIF\(reqs, 0\)                AS error_pct,"
            f"  CAST\(reqs AS BIGINT\)                             AS reqs"
            f" FROM read_parquet\(\[\{paths_sql\}\]\)"
            f" WHERE hour_ts >= TIMESTAMPTZ '\{st_iso\}' AND hour_ts < TIMESTAMPTZ '\{et_iso\}'"
            f" ORDER BY hour_ts, reqs DESC"
            f" LIMIT 5000"
        \)

        # Metro rows: aggregated across all hours — no time dimension.
        # Row shape matches METRO_LEADERBOARD output:
        #   \(country, city, region, metro, rtt_med_us, avg_ploss, error_pct, reqs\)
        metro_sql = \(
            f"SELECT"
            f"  country,"
            f"  city,"
            f"  CAST\('' AS VARCHAR\)                                   AS region,"
            f"  metro,"
            f"  SUM\(rtt_sum\) / NULLIF\(SUM\(rtt_count\), 0\)             AS rtt_med_us,"
            f"  SUM\(ploss_sum\) / NULLIF\(SUM\(ploss_count\), 0\)         AS avg_ploss,"
            f"  SUM\(errors\) \* 100.0 / NULLIF\(SUM\(reqs\), 0\)           AS error_pct,"
            f"  CAST\(SUM\(reqs\) AS BIGINT\)                             AS total_reqs"
            f" FROM read_parquet\(\[\{paths_sql\}\]\)"
            f" WHERE hour_ts >= TIMESTAMPTZ '\{st_iso\}' AND hour_ts < TIMESTAMPTZ '\{et_iso\}'"
            f" GROUP BY country, city, metro"
            f" ORDER BY total_reqs DESC"
            f" LIMIT 100"
        \)"""

geo_replace = r"""        from datetime import UTC, datetime
        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for
        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            map_sql = (
                f"SELECT * FROM ("
                f"  SELECT"
                f"    country, city, lat, lon, metro, hour_ts AS bucket_ts,"
                f"    rtt_sum / NULLIF(rtt_count, 0) AS rtt_med_us,"
                f"    ploss_sum / NULLIF(ploss_count, 0) AS avg_ploss,"
                f"    errors * 100.0 / NULLIF(reqs, 0) AS error_pct,"
                f"    CAST(reqs AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])"
                f"  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  UNION ALL "
                f"  SELECT"
                f"    client_geo_country_code AS country,"
                f"    client_geo_city AS city,"
                f"    CAST(client_geo_latitude AS DOUBLE) AS lat,"
                f"    CAST(client_geo_longitude AS DOUBLE) AS lon,"
                f"    client_geo_metro_code AS metro,"
                f"    CAST(TIMESTAMPTZ '{ah_iso}' AS TIMESTAMP) AS bucket_ts,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,"
                f"    0::DOUBLE AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs"
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
                f"    AND client_geo_country_code IS NOT NULL AND client_geo_country_code != ''"
                f"  GROUP BY country, city, lat, lon, metro"
                f") ORDER BY bucket_ts, reqs DESC LIMIT 5000"
            )
            metro_sql = (
                f"SELECT country, city, CAST('' AS VARCHAR) AS region, metro, "
                f"  CAST(SUM(rtt_med_us * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS rtt_med_us,"
                f"  CAST(SUM(avg_ploss * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS avg_ploss,"
                f"  CAST(SUM(error_pct * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS error_pct,"
                f"  CAST(SUM(reqs) AS BIGINT) AS total_reqs "
                f"FROM ("
                f"  SELECT country, city, metro,"
                f"    SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0) AS rtt_med_us,"
                f"    SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0) AS avg_ploss,"
                f"    SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0) AS error_pct,"
                f"    CAST(SUM(reqs) AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])"
                f"  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  GROUP BY country, city, metro"
                f"  UNION ALL "
                f"  SELECT client_geo_country_code AS country, client_geo_city AS city, client_geo_metro_code AS metro,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,"
                f"    0::DOUBLE AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs"
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
                f"    AND client_geo_country_code IS NOT NULL AND client_geo_country_code != ''"
                f"  GROUP BY country, city, metro"
                f") GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
            )
        else:
            map_sql = (
                f"SELECT"
                f"  country,"
                f"  city,"
                f"  lat,"
                f"  lon,"
                f"  metro,"
                f"  hour_ts                                           AS bucket_ts,"
                f"  rtt_sum / NULLIF(rtt_count, 0)                  AS rtt_med_us,"
                f"  ploss_sum / NULLIF(ploss_count, 0)              AS avg_ploss,"
                f"  errors * 100.0 / NULLIF(reqs, 0)                AS error_pct,"
                f"  CAST(reqs AS BIGINT)                             AS reqs"
                f" FROM read_parquet([{paths_sql}])"
                f" WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f" ORDER BY hour_ts, reqs DESC"
                f" LIMIT 5000"
            )

            # Metro rows: aggregated across all hours — no time dimension.
            # Row shape matches METRO_LEADERBOARD output:
            #   (country, city, region, metro, rtt_med_us, avg_ploss, error_pct, reqs)
            metro_sql = (
                f"SELECT"
                f"  country,"
                f"  city,"
                f"  CAST('' AS VARCHAR)                                   AS region,"
                f"  metro,"
                f"  SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0)             AS rtt_med_us,"
                f"  SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0)         AS avg_ploss,"
                f"  SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0)           AS error_pct,"
                f"  CAST(SUM(reqs) AS BIGINT)                             AS total_reqs"
                f" FROM read_parquet([{paths_sql}])"
                f" WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f" GROUP BY country, city, metro"
                f" ORDER BY total_reqs DESC"
                f" LIMIT 100"
            )"""

content = re.sub(geo_search, geo_replace, content)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
