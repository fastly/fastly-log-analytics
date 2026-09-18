import re

with open("backend/repositories/_base.py") as f:
    content = f.read()

speed_search = r"""        sql = \(
            f"SELECT asn, c_speed, CAST\(SUM\(count\) AS BIGINT\) AS cnt "
            f"FROM read_parquet\(\[\{paths_sql\}\]\) "
            f"WHERE asn IN \(\{asn_placeholders\}\) "
            f"GROUP BY asn, c_speed "
            f"ORDER BY asn, cnt DESC"
        \)"""

speed_replace = r"""        from datetime import UTC, datetime
        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        from backend.core.rollups import _safe_table_for
        base_table = _safe_table_for(self.src)

        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            et_iso = et.isoformat()
            sql = (
                f"SELECT asn, c_speed, CAST(SUM(cnt) AS BIGINT) AS cnt FROM ("
                f"  SELECT asn, c_speed, count AS cnt "
                f"  FROM read_parquet([{paths_sql}]) "
                f"  WHERE asn IN ({asn_placeholders}) "
                f"  UNION ALL "
                f"  SELECT asn, c_speed, CAST(COUNT(*) AS BIGINT) AS cnt "
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}' "
                f"    AND asn IN ({asn_placeholders}) AND c_speed IS NOT NULL "
                f"  GROUP BY asn, c_speed"
                f") GROUP BY asn, c_speed "
                f"ORDER BY asn, cnt DESC"
            )
            # Duplicate top_asns because asn_placeholders appears twice
            top_asns = top_asns + top_asns
        else:
            sql = (
                f"SELECT asn, c_speed, CAST(SUM(count) AS BIGINT) AS cnt "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE asn IN ({asn_placeholders}) "
                f"GROUP BY asn, c_speed "
                f"ORDER BY asn, cnt DESC"
            )"""

content = re.sub(speed_search, speed_replace, content)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
