"""Template-render tests for `backend.repositories._sql.network`.

Phase 5a — verifies the format-template structure (no DuckDB needed).
Each constant has a render test (assert expected fragments appear) and a
placeholder-set pin (prevent silent placeholder drift).
"""

from __future__ import annotations

from backend.repositories._sql import network as SQL


def _placeholders(template: str) -> list[str]:
    """Return the sorted list of ``{name}`` format placeholders in ``template``."""
    return sorted(p.split("}")[0] for p in template.split("{")[1:] if "}" in p)


# ── HEATMAP_BY_ASN_BUCKET ────────────────────────────────────────────────────


def test_heatmap_by_asn_bucket_renders_with_all_inputs():
    rendered = SQL.HEATMAP_BY_ASN_BUCKET.format(
        bucket_ms=300000,
        rtt_min_expr="MEDIAN(rtt_min)",
        congestion_expr="MEDIAN(COALESCE(tcp_rtt, 0) - COALESCE(rtt_min, 0))",
        ploss_expr="AVG(ploss)",
        rtt_var_expr="MEDIAN(rtt_var)",
        table='"_tmp_xyz"',
        where="1=1",
        row_limit=6000,
    )
    assert "EPOCH_MS" in rendered
    assert "APPROX_QUANTILE(tcp_rtt, 0.5)" in rendered
    assert "MEDIAN(rtt_min)" in rendered
    assert "AVG(ploss)" in rendered
    assert "MEDIAN(rtt_var)" in rendered
    assert 'FROM "_tmp_xyz"' in rendered
    assert "WHERE 1=1" in rendered
    # 2-pass CTE: top_cells groups bare (asn, bucket); the outer query
    # joins back and groups by the top_cells alias columns.
    assert "GROUP BY tc.asn, tc.bucket, tc.reqs, tc.err_count" in rendered
    assert "LIMIT 6000" in rendered
    # The bucket-ms value appears in one EPOCH_MS arithmetic block, which
    # uses the value twice (numerator + multiplier) — once in
    # the CTE. Total: 2 occurrences.
    assert rendered.count("300000") == 2


def test_heatmap_by_asn_bucket_renders_with_null_column_exprs():
    """When schema columns are absent, callers pass ``"NULL"`` — verify."""
    rendered = SQL.HEATMAP_BY_ASN_BUCKET.format(
        bucket_ms=60000,
        rtt_min_expr="NULL",
        congestion_expr="NULL",
        ploss_expr="NULL",
        rtt_var_expr="NULL",
        table='"_tmp_x"',
        where="1=1",
        row_limit=200,
    )
    assert "NULL           AS rtt_baseline_us" in rendered
    assert "NULL             AS avg_ploss" in rendered


def test_heatmap_by_asn_bucket_placeholders_pinned():
    # 2-pass CTE shape: precomputed CTE limits work, then the outer SELECT
    # joins back to the precomputed CTE. ``{bucket_ms}`` appears 2x
    # (one EPOCH_MS block, using numerator + multiplier),
    # ``{table}`` 4x (precomputed FROM, outer FROM as {table}, JOIN ON's two refs), and
    # ``{where}`` 2x (precomputed + top_cells).
    assert _placeholders(SQL.HEATMAP_BY_ASN_BUCKET) == sorted(
        [
            "bucket_ms",
            "bucket_ms",
            "rtt_min_expr",
            "congestion_expr",
            "ploss_expr",
            "rtt_var_expr",
            "table",
            "table",
            "table",
            "table",
            "where",
            "where",
            "row_limit",
        ]
    )


# ── MAP_BY_COUNTRY_BUCKET ────────────────────────────────────────────────────


def test_map_by_country_bucket_renders_with_all_inputs():
    rendered = SQL.MAP_BY_COUNTRY_BUCKET.format(
        city_col="city",
        lat_col="lat",
        lon_col="lon",
        metro_col="metro",
        join_city_col='"_tmp_x".city',
        join_lat_col='"_tmp_x".lat',
        join_lon_col='"_tmp_x".lon',
        join_metro_col='"_tmp_x".metro',
        bucket_ms=300000,
        ploss_expr="AVG(ploss)",
        table='"_tmp_x"',
        where="1=1",
    )
    assert 'APPROX_QUANTILE("_tmp_x".tcp_rtt, 0.5)' in rendered
    assert "AVG(ploss)" in rendered
    assert 'FROM "_tmp_x"' in rendered
    # 2-pass CTE: outer SELECT joins back to top_cells and groups by
    # its tc.* aliases (plus the carried-through tc.reqs / tc.err_count
    # so the divisions in error_pct don't break aggregation rules).
    assert "GROUP BY tc.country, tc.city, tc.lat, tc.lon, tc.metro, tc.bucket, tc.reqs, tc.err_count" in rendered
    assert "LIMIT 5000" in rendered
    assert "ORDER BY bucket, reqs DESC" in rendered
    # bucket_ms appears in one EPOCH_MS block (precomputed CTE),
    # using numerator + multiplier → 2 occurrences total post-optimization.
    assert rendered.count("300000") == 2


def test_map_by_country_bucket_renders_with_extended_where_for_map_asn():
    """Callers append ``" AND asn = ?"`` when ``map_asn`` is specified;
    the template's WHERE substitution must accept that shape."""
    rendered = SQL.MAP_BY_COUNTRY_BUCKET.format(
        city_col="city",
        lat_col="lat",
        lon_col="lon",
        metro_col="metro",
        join_city_col='"_tmp_x".city',
        join_lat_col='"_tmp_x".lat',
        join_lon_col='"_tmp_x".lon',
        join_metro_col='"_tmp_x".metro',
        bucket_ms=60000,
        ploss_expr="AVG(ploss)",
        table='"_tmp_x"',
        where="1=1 AND asn = ?",
    )
    assert "WHERE 1=1 AND asn = ?" in rendered


def test_map_by_country_bucket_placeholders_pinned():
    # 2-pass CTE shape: precomputed CTE projects bare column refs (city_col,
    # lat_col, lon_col, metro_col); the outer JOIN ON references the
    # same logical columns but qualified with the temp-table name to
    # disambiguate from top_cells' aliases — those are join_* siblings.
    # ``{bucket_ms}`` 2x (one EPOCH_MS block × 2 uses), ``{table}``
    # 6x (precomputed FROM, outer FROM as {table}, JOIN ON and WHERE refs), ``{where}`` 2x.
    assert _placeholders(SQL.MAP_BY_COUNTRY_BUCKET) == sorted(
        [
            "city_col",
            "lat_col",
            "lon_col",
            "metro_col",
            "join_city_col",
            "join_lat_col",
            "join_lon_col",
            "join_metro_col",
            "bucket_ms",
            "bucket_ms",
            "ploss_expr",
            "table",
            "table",
            "table",
            "table",
            "table",
            "table",
            "table",
            "where",
            "where",
        ]
    )


# ── METRO_LEADERBOARD ────────────────────────────────────────────────────────


def test_metro_leaderboard_renders_with_all_inputs():
    rendered = SQL.METRO_LEADERBOARD.format(
        city_col="city",
        region_col="region",
        metro_col="metro",
        join_city_col='"_tmp_x".city',
        join_region_col='"_tmp_x".region',
        join_metro_col='"_tmp_x".metro',
        ploss_expr="AVG(ploss)",
        table='"_tmp_x"',
        where="1=1",
    )
    assert "APPROX_QUANTILE(tcp_rtt, 0.5)" in rendered
    assert "AVG(ploss)" in rendered
    assert 'FROM "_tmp_x"' in rendered
    # 2-pass CTE: outer SELECT groups by tc.* (top_cells aliases) plus
    # the carried-through tc.reqs / tc.err_count.
    assert "GROUP BY tc.country, tc.city, tc.region, tc.metro, tc.reqs, tc.err_count" in rendered
    assert "LIMIT 100" in rendered


def test_metro_leaderboard_placeholders_pinned():
    # Same shape as MAP_BY_COUNTRY_BUCKET above: bare *_col in the CTE
    # projection, join_*_col in the JOIN ON. No bucket_ms here (no time
    # bucketing — top-100 by total reqs). ``{table}`` 3x, ``{where}`` 2x.
    assert _placeholders(SQL.METRO_LEADERBOARD) == sorted(
        [
            "city_col",
            "region_col",
            "metro_col",
            "join_city_col",
            "join_region_col",
            "join_metro_col",
            "ploss_expr",
            "table",
            "table",
            "table",
            "where",
            "where",
        ]
    )


# ── SPEED_DISTRIBUTION_BY_ASN ────────────────────────────────────────────────


def test_speed_distribution_by_asn_renders_with_placeholders():
    rendered = SQL.SPEED_DISTRIBUTION_BY_ASN.format(
        table='"_tmp_x"',
        where="1=1",
        placeholders="?,?,?",
    )
    assert "SELECT asn, c_speed, COUNT(*)" in rendered
    assert "asn IN (?,?,?)" in rendered
    assert 'FROM "_tmp_x"' in rendered
    assert "GROUP BY asn, c_speed" in rendered


def test_speed_distribution_by_asn_placeholders_pinned():
    assert _placeholders(SQL.SPEED_DISTRIBUTION_BY_ASN) == sorted(
        [
            "table",
            "where",
            "placeholders",
        ]
    )


# ── RTT_PERCENTILES_BY_ASN ───────────────────────────────────────────────────


def test_rtt_percentiles_by_asn_renders_with_placeholders():
    rendered = SQL.RTT_PERCENTILES_BY_ASN.format(
        table='"_tmp_x"',
        where="1=1",
        placeholders="?,?",
    )
    assert "APPROX_QUANTILE(tcp_rtt, 0.95)" in rendered
    assert "APPROX_QUANTILE(tcp_rtt, 0.99)" in rendered
    assert "asn IN (?,?)" in rendered
    assert "GROUP BY asn" in rendered


def test_rtt_percentiles_by_asn_placeholders_pinned():
    assert _placeholders(SQL.RTT_PERCENTILES_BY_ASN) == sorted(
        [
            "table",
            "where",
            "placeholders",
        ]
    )


# ── QUALITY_BAR_BY_GROUP ─────────────────────────────────────────────────────


def test_quality_bar_by_group_renders_without_extra_where():
    rendered = SQL.QUALITY_BAR_BY_GROUP.format(
        group_col="country",
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where="",
    )
    assert '"country" AS label' in rendered
    assert "APPROX_QUANTILE(tcp_rtt, 0.5) / 1000.0 AS rtt_ms" in rendered
    assert 'GROUP BY "country"' in rendered
    assert "LIMIT 25" in rendered
    assert "ORDER BY reqs DESC" in rendered


def test_quality_bar_by_group_renders_with_extra_where():
    """The region rollup appends ``" AND country = ?"`` and binds a value."""
    rendered = SQL.QUALITY_BAR_BY_GROUP.format(
        group_col="region",
        table='"logs_xyz"',
        rtt_filter="tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where=" AND country = ?",
    )
    assert "WHERE tcp_rtt IS NOT NULL AND tcp_rtt > 0 AND country = ?" in rendered
    assert '"region" AS label' in rendered


def test_quality_bar_by_group_placeholders_pinned():
    assert _placeholders(SQL.QUALITY_BAR_BY_GROUP) == sorted(
        [
            "group_col",
            "group_col",
            "group_col",
            "group_col",
            "table",
            "rtt_filter",
            "extra_where",
        ]
    )


# ── QUALITY_COUNTRIES_DISTINCT ───────────────────────────────────────────────


def test_quality_countries_distinct_renders_with_all_inputs():
    rendered = SQL.QUALITY_COUNTRIES_DISTINCT.format(
        table='"logs_xyz"',
        where_clause="ts BETWEEN '2026-01-01' AND '2026-01-02'",
    )
    assert "SELECT DISTINCT country" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "country IS NOT NULL AND country != ''" in rendered
    assert "ORDER BY country" in rendered


def test_quality_countries_distinct_placeholders_pinned():
    assert _placeholders(SQL.QUALITY_COUNTRIES_DISTINCT) == sorted(
        [
            "table",
            "where_clause",
        ]
    )


# ── QUALITY_SCATTER ──────────────────────────────────────────────────────────


def test_quality_scatter_renders_with_all_inputs():
    rendered = SQL.QUALITY_SCATTER.format(
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
    )
    assert "tcp_rtt / 1000.0 AS rtt_ms" in rendered
    assert "ttfb * 1000.0 AS ttfb_ms" in rendered
    assert "COALESCE(cache, 'UNKNOWN') AS cache_state" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "USING SAMPLE 2000" in rendered
    assert "ttfb IS NOT NULL AND ttfb > 0" in rendered


def test_quality_scatter_placeholders_pinned():
    assert _placeholders(SQL.QUALITY_SCATTER) == sorted(
        [
            "table",
            "rtt_filter",
        ]
    )
