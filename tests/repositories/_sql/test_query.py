"""Template-render tests for `backend.repositories._sql.query`.

Phase 5a — verifies the format-template structure (no DuckDB needed).
These tests are string-level only; behavioural coverage lives in
``tests/repositories/test_query.py``.
"""

from __future__ import annotations

from backend.repositories._sql import query as SQL


def _placeholders(template: str) -> list[str]:
    return sorted(
        p.split("}")[0]
        for p in template.split("{")[1:]
        if "}" in p
    )


# ── EXPLAIN_WRAPPER ───────────────────────────────────────────────────────────


def test_explain_wrapper_renders_with_user_sql():
    rendered = SQL.EXPLAIN_WRAPPER.format(sql="SELECT 1")
    assert rendered == "EXPLAIN SELECT 1"


def test_explain_wrapper_pins_placeholders():
    assert _placeholders(SQL.EXPLAIN_WRAPPER) == ["sql"]


# ── AUTO_LIMIT_WRAPPER ────────────────────────────────────────────────────────


def test_auto_limit_wrapper_renders_with_inner_and_limit():
    rendered = SQL.AUTO_LIMIT_WRAPPER.format(
        inner="SELECT * FROM logs_svc ORDER BY id",
        limit=1001,
    )
    assert "SELECT * FROM (SELECT * FROM logs_svc ORDER BY id) AS _q" in rendered
    assert "LIMIT 1001" in rendered


def test_auto_limit_wrapper_uses_underscore_q_alias():
    """The ``AS _q`` alias is load-bearing — the wrapper relies on it for
    DuckDB to plan the outer LIMIT against the inner SELECT's top-k."""
    rendered = SQL.AUTO_LIMIT_WRAPPER.format(inner="SELECT 1", limit=10)
    assert "AS _q" in rendered


def test_auto_limit_wrapper_pins_placeholders():
    assert _placeholders(SQL.AUTO_LIMIT_WRAPPER) == ["inner", "limit"]


# ── PRESET_SAMPLE_ROWS ────────────────────────────────────────────────────────


def test_preset_sample_rows_renders_with_table():
    rendered = SQL.PRESET_SAMPLE_ROWS.format(table="logs_myservice")
    assert rendered == "SELECT * FROM logs_myservice LIMIT 100"


def test_preset_sample_rows_has_no_order_by():
    """Regression pin — the preset must NOT force a sort on the
    full table (would make the preview feel broken on 1.6M-row tables)."""
    rendered = SQL.PRESET_SAMPLE_ROWS.format(table="logs_myservice")
    assert "ORDER BY" not in rendered.upper()


def test_preset_sample_rows_pins_placeholders():
    assert _placeholders(SQL.PRESET_SAMPLE_ROWS) == ["table"]


# ── PRESET_ROW_COUNT ──────────────────────────────────────────────────────────


def test_preset_row_count_renders_with_table():
    rendered = SQL.PRESET_ROW_COUNT.format(table="logs_myservice")
    assert rendered == "SELECT count(*) AS total_rows FROM logs_myservice"


def test_preset_row_count_pins_placeholders():
    assert _placeholders(SQL.PRESET_ROW_COUNT) == ["table"]


# ── PRESET_COLUMN_STATS ───────────────────────────────────────────────────────


def test_preset_column_stats_renders_with_table():
    rendered = SQL.PRESET_COLUMN_STATS.format(table="logs_myservice")
    assert rendered == "SUMMARIZE logs_myservice"


def test_preset_column_stats_pins_placeholders():
    assert _placeholders(SQL.PRESET_COLUMN_STATS) == ["table"]
