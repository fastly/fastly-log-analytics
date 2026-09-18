import re

with open("backend/repositories/_base.py") as f:
    content = f.read()

rtt_search = r"""        sql = \(
            f"SELECT "
            f"  asn, "
            f"  CAST\(SUM\(p95_rtt_us \* requests\) / NULLIF\(SUM\(requests\), 0\) AS DOUBLE\), "
            f"  CAST\(SUM\(p99_rtt_us \* requests\) / NULLIF\(SUM\(requests\), 0\) AS DOUBLE\) "
            f"FROM read_parquet\(\[\{paths_sql\}\]\) "
            f"WHERE asn IN \(\{asn_placeholders\}\) "
            f"GROUP BY asn"
        \)"""

rtt_replace = r"""        from datetime import UTC, datetime
        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for
        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            et_iso = et.isoformat()
            sql = (
                f"SELECT "
                f"  asn, "
                f"  CAST(SUM(p95_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE), "
                f"  CAST(SUM(p99_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM ("
                f"  SELECT asn, requests, p95_rtt_us, p99_rtt_us "
                f"  FROM read_parquet([{paths_sql}]) "
                f"  WHERE asn IN ({asn_placeholders}) "
                f"  UNION ALL "
                f"  SELECT asn, CAST(COUNT(*) AS BIGINT) AS requests, "
                f"         CAST(approx_quantile(tcp_rtt, 0.95) AS DOUBLE) AS p95_rtt_us, "
                f"         CAST(approx_quantile(tcp_rtt, 0.99) AS DOUBLE) AS p99_rtt_us "
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}' "
                f"    AND asn IN ({asn_placeholders}) AND tcp_rtt IS NOT NULL "
                f"  GROUP BY asn "
                f") "
                f"GROUP BY asn"
            )
            top_asns = top_asns + top_asns
        else:
            sql = (
                f"SELECT "
                f"  asn, "
                f"  CAST(SUM(p95_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE), "
                f"  CAST(SUM(p99_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE asn IN ({asn_placeholders}) "
                f"GROUP BY asn"
            )"""

content = re.sub(rtt_search, rtt_replace, content)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
