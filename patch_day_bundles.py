def _patch():
    with open("backend/core/rollups/day_bundles.py") as f:
        content = f.read()

    new_compactor = """
def compact_network_quality_closed_days_to_daily(service_id: str, source: dict) -> int:
    from ._common import (
        NETWORK_QUALITY_COUNTRY_FILENAME,
        NETWORK_QUALITY_ASN_FILENAME,
        NETWORK_QUALITY_REGION_FILENAME,
        NETWORK_QUALITY_POP_FILENAME,
    )

    def _build_sql(dim_col: str, include_country: bool):
        country_select = ", country" if include_country else ""
        return lambda paths_sql, tmp_file: (
            f"COPY ("
            f"  SELECT dim_val {country_select}, "
            f"    CAST(SUM(requests) AS BIGINT) AS requests, "
            f"    CAST(SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) AS p50_us "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY dim_val {country_select} "
            f"  ORDER BY requests DESC LIMIT 100"
            f") TO '{tmp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[
            (NETWORK_QUALITY_COUNTRY_FILENAME, ".tmp_nq_", _build_sql("country", False)),
            (NETWORK_QUALITY_ASN_FILENAME, ".tmp_nq_", _build_sql("asn", False)),
            (NETWORK_QUALITY_REGION_FILENAME, ".tmp_nq_", _build_sql("region", True)),
            (NETWORK_QUALITY_POP_FILENAME, ".tmp_nq_", _build_sql("pop", False)),
        ],
        logger=logger,
    )
"""
    content += "\n" + new_compactor + "\n"

    with open("backend/core/rollups/day_bundles.py", "w") as f:
        f.write(content)


_patch()
