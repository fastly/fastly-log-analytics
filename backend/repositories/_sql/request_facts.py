"""SQL templates for `backend.repositories.request_facts` (``{table}`` is a ``_safe_table_name`` identifier)."""

from __future__ import annotations

DESCRIBE_TABLE = "DESCRIBE {table}"

EMULATION_PROBE = "SELECT 1 AS duckdb_emulation_probe"

EXTENTS = "SELECT MIN(timestamp), MAX(timestamp) FROM {table}"
