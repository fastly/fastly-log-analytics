"""Tests for `backend.repositories._sql.usage` templates.

Phase 5a — verifies the format-template structure (no DuckDB needed).
"""

from __future__ import annotations

from backend.repositories._sql import usage as SQL


def test_edge_ratio_pct_renders_with_table_name():
    rendered = SQL.EDGE_RATIO_PCT.format(table='"logs_xyz"')
    assert "count(*) FILTER (WHERE edge = true)" in rendered
    assert 'FROM "logs_xyz"' in rendered


def test_edge_ratio_pct_template_has_no_raw_user_input_placeholders():
    """The only format placeholder is ``{table}`` (trusted identifier).
    A SQL parameter binding would use ``?``, not a format placeholder."""
    placeholders = [
        p for p in SQL.EDGE_RATIO_PCT.split("{")[1:]
        if "}" in p
    ]
    names = [p.split("}")[0] for p in placeholders]
    assert names == ["table"]
