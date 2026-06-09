"""Template-render tests for `backend.repositories._sql.performance`."""

from __future__ import annotations

from backend.repositories._sql import performance as SQL


def test_origin_timeseries_renders_with_all_inputs():
    rendered = SQL.ORIGIN_TIMESERIES.format(
        time_bucket_select="time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
        value_expr="ROUND(PERCENTILE_CONT(0.95) ...)",
        table='"logs_xyz"',
        where_clause="timestamp BETWEEN '2026-06-09T00:00:00Z' AND '2026-06-09T01:00:00Z'",
        metric_col="ottfb",
    )
    assert "time_bucket(INTERVAL '1 minute', timestamp)" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "GROUP BY 1 ORDER BY 1" in rendered
    assert '"ottfb" IS NOT NULL' in rendered


def test_origin_timeseries_template_pins_all_expected_placeholders():
    placeholders = sorted(
        p.split("}")[0]
        for p in SQL.ORIGIN_TIMESERIES.split("{")[1:]
        if "}" in p
    )
    assert placeholders == sorted([
        "time_bucket_select",
        "value_expr",
        "table",
        "where_clause",
        "metric_col",
    ])
