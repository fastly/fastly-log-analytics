"""SQL templates for `backend.repositories.usage`.

Phase 5a extraction. See ``backend/repositories/_sql/__init__.py`` for the
ownership policy.

Each template is a Python format string. The format placeholders are
trusted-identifier substitutions only (table name, column name); user
input is bound via DuckDB parameter binding, not string interpolation.
"""

from __future__ import annotations

# ── Edge ratio ────────────────────────────────────────────────────────────────

EDGE_RATIO_PCT = "SELECT count(*) FILTER (WHERE edge = true) * 100.0 / count(*) FROM {table}"
"""Percentage of rows where ``edge = true``.

Inputs:
- ``{table}`` — quoted table identifier (e.g. result of ``_safe_table_name``).

Output (one row):
- column 0: float | None (None when table is empty)
"""

__all__ = ["EDGE_RATIO_PCT"]
