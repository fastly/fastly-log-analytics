"""Template-render tests for `backend.repositories._sql.origin`.

Phase 5a — verifies the format-template structure (no DuckDB needed).
Each constant gets a render assertion + a placeholder-set pin so a
silent rename of one placeholder fails loudly.
"""

from __future__ import annotations

from backend.repositories._sql import origin as SQL


def _placeholders(template: str) -> list[str]:
    """Extract format-style ``{name}`` placeholders from a SQL template."""
    return sorted({p.split("}")[0] for p in template.split("{")[1:] if "}" in p})


# ── Live-table templates ──────────────────────────────────────────────────────


def test_summary_grouping_sets_renders():
    rendered = SQL.SUMMARY_GROUPING_SETS.format(
        edge_select='"edge"',
        grouping_expr='GROUPING("edge")',
        lat_val='COALESCE("ottfb", "ttfb" * 1000000.0)',
        ottlb_p50='MEDIAN("ottlb") / 1000.0',
        ottlb_p95='APPROX_QUANTILE("ottlb", 0.95) / 1000.0',
        cdn_ovh='MEDIAN("elapsed" - "ottlb") / 1000.0',
        ost_5xx='COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)',
        obytes_p50='MEDIAN("obytes")',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
        grouping_clause='GROUP BY GROUPING SETS ((), ("edge"))',
    )
    assert 'FROM "logs_xyz"' in rendered
    assert 'GROUPING("edge")' in rendered
    assert 'MEDIAN(COALESCE("ottfb", "ttfb" * 1000000.0)) / 1000.0' in rendered
    assert "GROUP BY GROUPING SETS" in rendered
    assert "AS ottfb_p99_ms" in rendered


def test_summary_grouping_sets_placeholders_pinned():
    assert _placeholders(SQL.SUMMARY_GROUPING_SETS) == sorted(
        [
            "edge_select",
            "grouping_expr",
            "lat_val",
            "ottlb_p50",
            "ottlb_p95",
            "cdn_ovh",
            "ost_5xx",
            "obytes_p50",
            "table",
            "where",
            "grouping_clause",
        ]
    )


def test_timeseries_bucketed_renders():
    rendered = SQL.TIMESERIES_BUCKETED.format(
        interval="INTERVAL '5' minutes",
        agg_expr='APPROX_QUANTILE("ottfb", 0.95)',
        unit_conv="/ 1000.0",
        edge_col=', "edge"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
        lat_expr='"ottfb"',
        edge_group=', "edge"',
    )
    assert "time_bucket(INTERVAL '5' minutes, \"timestamp\")" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert 'APPROX_QUANTILE("ottfb", 0.95) / 1000.0' in rendered
    assert "GROUP BY ts" in rendered
    assert "ORDER BY ts" in rendered


def test_timeseries_bucketed_placeholders_pinned():
    assert _placeholders(SQL.TIMESERIES_BUCKETED) == sorted(
        [
            "interval",
            "agg_expr",
            "unit_conv",
            "edge_col",
            "table",
            "where",
            "lat_expr",
            "edge_group",
        ]
    )


def test_slow_urls_renders():
    rendered = SQL.SLOW_URLS.format(
        lat_val='COALESCE("ottfb", "ttfb" * 1000000.0)',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'SELECT\n          "url"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "HAVING COUNT(*) >= ?" in rendered
    assert rendered.rstrip().endswith("LIMIT ?")


def test_slow_urls_placeholders_pinned():
    assert _placeholders(SQL.SLOW_URLS) == sorted(["lat_val", "table", "where"])


def test_status_codes_renders():
    rendered = SQL.STATUS_CODES.format(
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert '"ost"' in rendered
    assert "OVER ()" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "ORDER BY count DESC" in rendered


def test_status_codes_placeholders_pinned():
    assert _placeholders(SQL.STATUS_CODES) == sorted(["table", "where"])


def test_path_breakdown_renders():
    rendered = SQL.PATH_BREAKDOWN.format(
        lat_val='"ottfb"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'GROUP BY "edge"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert 'MEDIAN("ottfb")' in rendered


def test_path_breakdown_placeholders_pinned():
    assert _placeholders(SQL.PATH_BREAKDOWN) == sorted(["lat_val", "table", "where"])


def test_pop_latency_renders():
    rendered = SQL.POP_LATENCY.format(
        lat_val='"ottfb"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'GROUP BY "pop"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "ORDER BY p95_ms DESC" in rendered
    assert rendered.rstrip().endswith("LIMIT ?")


def test_pop_latency_placeholders_pinned():
    assert _placeholders(SQL.POP_LATENCY) == sorted(["lat_val", "table", "where"])


def test_ip_health_renders():
    rendered = SQL.IP_HEALTH.format(
        lat_val='"ottfb"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'GROUP BY "oip"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "HAVING COUNT(*) >= 10" in rendered
    assert "ORDER BY error_pct DESC" in rendered
    assert rendered.rstrip().endswith("LIMIT ?")


def test_ip_health_placeholders_pinned():
    assert _placeholders(SQL.IP_HEALTH) == sorted(["lat_val", "table", "where"])


def test_shielding_analysis_renders():
    rendered = SQL.SHIELDING_ANALYSIS.format(
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
        time_where="timestamp BETWEEN ? AND ?",
    )
    assert "WITH edge_logs AS" in rendered
    assert "shield_logs AS" in rendered
    assert "INNER JOIN shield_logs s ON s.prid = e.rid" in rendered
    assert "PERCENTILE_CONT(0.50)" in rendered
    assert rendered.rstrip().endswith("LIMIT ?")


def test_shielding_analysis_placeholders_pinned():
    assert _placeholders(SQL.SHIELDING_ANALYSIS) == sorted(["table", "where", "time_where"])


# ── Composite (TEMP TABLE) templates ──────────────────────────────────────────


def test_aggregates_create_temp_renders():
    rendered = SQL.AGGREGATES_CREATE_TEMP.format(
        temp_table="t_origin_deadbeef",
        select_cols='"timestamp", "cache", "edge"',
        lat_us_expr='COALESCE("ottfb", "ttfb" * 1000000.0)',
        table='"logs_xyz"',
        where_clause="timestamp BETWEEN '2026-06-09T00:00:00Z' AND '2026-06-09T01:00:00Z'",
    )
    assert rendered.startswith("CREATE TEMP TABLE t_origin_deadbeef AS ")
    assert 'SELECT "timestamp", "cache", "edge",' in rendered
    assert 'COALESCE("ottfb", "ttfb" * 1000000.0) AS lat_us' in rendered
    assert 'FROM "logs_xyz" WHERE' in rendered


def test_aggregates_create_temp_placeholders_pinned():
    assert _placeholders(SQL.AGGREGATES_CREATE_TEMP) == sorted(
        [
            "temp_table",
            "select_cols",
            "lat_us_expr",
            "table",
            "where_clause",
        ]
    )


# TEMP_SUMMARY_ROLLUP, TEMP_SUMMARY_BY_EDGE and the per-card TEMP mirrors
# (TEMP_TIMESERIES / TEMP_SLOW_URLS / TEMP_STATUS_CODES /
# TEMP_PATH_BREAKDOWN / TEMP_POP_LATENCY / TEMP_IP_HEALTH) were all
# deleted. Their consumers in :mod:`backend.repositories.origin` now
# render the live templates (SUMMARY_GROUPING_SETS, SLOW_URLS,
# PATH_BREAKDOWN, POP_LATENCY, IP_HEALTH, STATUS_CODES,
# TIMESERIES_BUCKETED) with ``table=<temp_table>``, ``where='1=1'``,
# ``lat_val='lat_us'`` — see ``_shape_summary`` in that module. The
# live-template render + placeholder tests above pin the SQL shape both
# call sites depend on.
