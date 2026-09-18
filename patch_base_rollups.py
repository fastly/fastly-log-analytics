import re

with open("backend/repositories/_base.py") as f:
    content = f.read()

# 1. try_pop_health_from_rollup
pop_health_search = r"""        query = \(
            f"SELECT "
            f"  pop, "
            f"  CAST\(SUM\(requests\) AS BIGINT\), "
            f"  CAST\(SUM\(errors\) AS BIGINT\), "
            f"  CAST\(SUM\(cache_hits\) AS BIGINT\), "
            f"  CAST\(SUM\(bandwidth_bytes\) AS BIGINT\), "
            f"  CAST\(SUM\(p50_rtt_us \* requests\) / NULLIF\(SUM\(requests\), 0\) AS DOUBLE\), "
            f"  CAST\(SUM\(p95_ttfb_ms \* requests\) / NULLIF\(SUM\(requests\), 0\) AS DOUBLE\) "
            f"FROM read_parquet\(\[\{paths_sql\}\]\) "
            f"WHERE pop IS NOT NULL AND pop != '' "
            f"GROUP BY pop"
        \)"""

pop_health_replace = r"""        from datetime import UTC, datetime
        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for
        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            et_iso = et.isoformat()
            query = (
                f"SELECT pop,"
                f"  CAST(SUM(requests) AS BIGINT),"
                f"  CAST(SUM(errors) AS BIGINT),"
                f"  CAST(SUM(cache_hits) AS BIGINT),"
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT),"
                f"  CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE),"
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM ("
                f"  SELECT pop, requests, errors, cache_hits, bandwidth_bytes, p50_rtt_us, p95_ttfb_ms "
                f"  FROM read_parquet([{paths_sql}]) "
                f"  WHERE pop IS NOT NULL AND pop != '' "
                f"  UNION ALL "
                f"  SELECT pop, CAST(COUNT(*) AS BIGINT) AS requests, "
                f"         CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS BIGINT) AS errors, "
                f"         CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS cache_hits, "
                f"         CAST(SUM(resp_bytes) AS BIGINT) AS bandwidth_bytes, "
                f"         CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS p50_rtt_us, "
                f"         CAST(approx_quantile(ttfb, 0.95) AS DOUBLE) AS p95_ttfb_ms "
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}' "
                f"    AND pop IS NOT NULL AND pop != '' "
                f"  GROUP BY pop"
                f") "
                f"GROUP BY pop"
            )
        else:
            query = (
                f"SELECT "
                f"  pop, "
                f"  CAST(SUM(requests) AS BIGINT), "
                f"  CAST(SUM(errors) AS BIGINT), "
                f"  CAST(SUM(cache_hits) AS BIGINT), "
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT), "
                f"  CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE), "
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE pop IS NOT NULL AND pop != '' "
                f"GROUP BY pop"
            )"""

content = re.sub(pop_health_search, pop_health_replace, content)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
