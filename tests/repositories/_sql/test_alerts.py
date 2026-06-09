"""Template-render tests for `backend.repositories._sql.alerts`.

Phase 5a — verifies the format-template structure (no DuckDB needed).
"""

from __future__ import annotations

from backend.repositories._sql import alerts as SQL

# ── MAX_TIMESTAMP ────────────────────────────────────────────────────────────


def test_max_timestamp_renders_with_table_name():
    rendered = SQL.MAX_TIMESTAMP.format(table='"logs_xyz"')
    assert rendered == 'SELECT max(timestamp) FROM "logs_xyz"'


def test_max_timestamp_template_pins_only_table_placeholder():
    placeholders = sorted(
        p.split("}")[0]
        for p in SQL.MAX_TIMESTAMP.split("{")[1:]
        if "}" in p
    )
    assert placeholders == ["table"]


# ── COUNT_REQUESTS_IN_WINDOW ─────────────────────────────────────────────────


def test_count_requests_in_window_renders_with_all_inputs():
    rendered = SQL.COUNT_REQUESTS_IN_WINDOW.format(
        table='"logs_xyz"',
        window_start_expr="(SELECT max(timestamp) FROM \"logs_xyz\") - INTERVAL '5 minutes'",
        window_end_expr='(SELECT max(timestamp) FROM "logs_xyz")',
    )
    assert "SELECT count(*) FROM \"logs_xyz\"" in rendered
    assert "WHERE timestamp >=" in rendered
    assert "AND timestamp <=" in rendered
    assert "INTERVAL '5 minutes'" in rendered


def test_count_requests_in_window_template_pins_all_expected_placeholders():
    placeholders = sorted(
        p.split("}")[0]
        for p in SQL.COUNT_REQUESTS_IN_WINDOW.split("{")[1:]
        if "}" in p
    )
    assert placeholders == sorted(["table", "window_start_expr", "window_end_expr"])


# ── MAX_TIMESTAMP_SUBQUERY_EXPR ──────────────────────────────────────────────


def test_max_timestamp_subquery_expr_renders_as_parenthesised_subquery():
    rendered = SQL.MAX_TIMESTAMP_SUBQUERY_EXPR.format(table='"logs_xyz"')
    assert rendered == '(SELECT max(timestamp) FROM "logs_xyz")'
    # Suitable for embedding inside a larger query without breaking precedence.
    assert rendered.startswith("(") and rendered.endswith(")")


def test_max_timestamp_subquery_expr_template_pins_only_table_placeholder():
    placeholders = sorted(
        p.split("}")[0]
        for p in SQL.MAX_TIMESTAMP_SUBQUERY_EXPR.split("{")[1:]
        if "}" in p
    )
    assert placeholders == ["table"]


# ── WINDOW_OFFSET_EXPR ───────────────────────────────────────────────────────


def test_window_offset_expr_renders_with_table_and_minutes():
    rendered = SQL.WINDOW_OFFSET_EXPR.format(table='"logs_xyz"', minutes_ago=15)
    assert (
        rendered
        == "(SELECT max(timestamp) FROM \"logs_xyz\") - INTERVAL '15 minutes'"
    )


def test_window_offset_expr_accepts_summed_minutes_for_historic_window():
    """Historic-window start uses ``comp_period + window`` for ``minutes_ago``
    — pin that arithmetic results render correctly (no quoting issues)."""
    rendered = SQL.WINDOW_OFFSET_EXPR.format(table='"logs_xyz"', minutes_ago=60 + 5)
    assert "INTERVAL '65 minutes'" in rendered


def test_window_offset_expr_template_pins_all_expected_placeholders():
    placeholders = sorted(
        p.split("}")[0]
        for p in SQL.WINDOW_OFFSET_EXPR.split("{")[1:]
        if "}" in p
    )
    assert placeholders == sorted(["table", "minutes_ago"])
