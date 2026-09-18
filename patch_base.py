def _patch_base():
    with open("backend/repositories/_base.py") as f:
        content = f.read()

    new_method = """
    def try_network_quality_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        region_country: str,
    ) -> dict[str, Any] | None:
        from backend.core.rollups._common import (
            NETWORK_QUALITY_COUNTRY_FILENAME,
            NETWORK_QUALITY_ASN_FILENAME,
            NETWORK_QUALITY_REGION_FILENAME,
            NETWORK_QUALITY_POP_FILENAME,
        )

        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=None, min_hours=24
        )
        if win is None:
            return None
        st, et = win

        def _run_dim(filename: str, group_col: str, filter_sql: str = "", filter_params: list = None) -> list[dict]:
            rollup_paths = self._collect_rollup_paths(st, et, filename)
            if not rollup_paths:
                return None
            paths_sql = ", ".join(f"'{p}'" for p in rollup_paths)

            sql = (
                f"SELECT dim_val as label, "
                f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) / 1000.0 AS rtt_ms, "
                f"       SUM(requests) AS reqs "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE 1=1 {filter_sql} "
                f"GROUP BY dim_val "
                f"ORDER BY reqs DESC "
                f"LIMIT 25"
            )
            rows = self.execute(sql, filter_params or []).fetchall()
            return [
                {"value": str(r[0]), "label": str(r[0]), "rtt_ms": round(float(r[1]), 2), "reqs": int(r[2])}
                for r in rows if r[1] is not None
            ]

        try:
            by_country = _run_dim(NETWORK_QUALITY_COUNTRY_FILENAME, "country")
            if by_country is None: return None

            by_asn = _run_dim(NETWORK_QUALITY_ASN_FILENAME, "asn")
            if by_asn is None: return None

            by_pop = _run_dim(NETWORK_QUALITY_POP_FILENAME, "pop")
            if by_pop is None: return None

            by_region = _run_dim(NETWORK_QUALITY_REGION_FILENAME, "region", "AND country = ?", [region_country])
            if by_region is None: return None

            return {
                "available": True,
                "by_country": by_country,
                "by_asn": by_asn,
                "by_pop": by_pop,
                "by_region": by_region,
                "region_country": region_country,
                "scatter": [],  # Scatter not supported in rollup; UI will just render empty or we handle it
                "countries": [], # Frontend gets countries from network-health map anyway!
                "_approx": True,
            }
        except Exception as e:
            import logging
            logging.getLogger(__name__).debug("[network_quality_rollup] read failed: %s", e)
            return None
"""
    # Insert it right before `try_network_rtt_from_rollup`
    content = content.replace(
        "    def try_network_rtt_from_rollup(", new_method + "\n    def try_network_rtt_from_rollup("
    )

    with open("backend/repositories/_base.py", "w") as f:
        f.write(content)


_patch_base()
