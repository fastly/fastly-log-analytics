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
    placeholders = sorted(p.split("}")[0] for p in SQL.MAX_TIMESTAMP.split("{")[1:] if "}" in p)
    assert placeholders == ["table"]
