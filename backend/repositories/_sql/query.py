"""SQL templates for `backend.repositories.query`.

Phase 5a extraction. The query repository owns the user-facing SQL
execution surface (analyst's SQL textarea + preset library). Templates
here are deliberately small wrappers — the *body* of the SQL is supplied
by the caller (validated user input or a built-in preset) and bound
through ``str.format``.

Security note: user-supplied SQL passes through
``backend.utils.sql_validator.validate_user_sql`` before it ever reaches
these wrappers. The ``{sql}`` / ``{inner}`` placeholders below are
therefore "trusted post-validation"; they are NOT a DuckDB ``?``
parameter substitution channel.

See ``pending-docs/sql_ownership_audit.md`` for the migration shape and
``backend/repositories/_sql/__init__.py`` for the ownership policy.
"""

from __future__ import annotations

# ── User-query wrappers ───────────────────────────────────────────────────────

EXPLAIN_WRAPPER = "EXPLAIN {sql}"
"""DuckDB EXPLAIN of an already-validated user SQL statement.

Inputs:
- ``{sql}`` — the user's SQL, post-``validate_user_sql``. Trusted.

Output: one row per plan line; the caller joins column 1 of each row.
"""


AUTO_LIMIT_WRAPPER = "SELECT * FROM ({inner}) AS _q LIMIT {limit}"
"""Auto-apply ``LIMIT max_rows+1`` to a simple SELECT.

Inputs:
- ``{inner}`` — the user's SELECT, trailing semicolon stripped, already
  validated by ``is_simple_select_statement`` and ``validate_user_sql``.
- ``{limit}`` — integer literal (``max_rows + 1``). The ``+1`` lets the
  caller detect truncation without a separate ``COUNT(*)`` pass and lets
  DuckDB's top-k optimiser kick in on ``ORDER BY ... LIMIT``.

Output: passes through the user query's columns, capped at ``{limit}``
rows.
"""


# ── Preset library ────────────────────────────────────────────────────────────
#
# Three fixed presets surface in the analyst's "Presets" dropdown. Each
# template substitutes only the trusted table identifier (output of
# ``_safe_table`` — strict ``[A-Za-z0-9_]+`` regex).

PRESET_SAMPLE_ROWS = "SELECT * FROM {table} LIMIT 100"
"""Preview 100 raw log rows.

Inputs:
- ``{table}`` — quoted/safe table identifier (output of ``_safe_table``).

Output: up to 100 rows, no ORDER BY (deliberately — a full sort on a
1.6M-row table made the preset feel broken, and the ORDER BY text
leaked into the analyst's textarea where editing ``*`` to ``COUNT(*)``
produced a Binder error).
"""


PRESET_ROW_COUNT = "SELECT count(*) AS total_rows FROM {table}"
"""Total number of rows in the log table.

Inputs:
- ``{table}`` — quoted/safe table identifier.

Output (one row): ``(total_rows: int)``.
"""


PRESET_COLUMN_STATS = "SUMMARIZE {table}"
"""DuckDB SUMMARIZE — per-column non-null counts, unique counts, etc.

Inputs:
- ``{table}`` — quoted/safe table identifier.

Output: one row per column with DuckDB's standard SUMMARIZE columns
(``column_name``, ``column_type``, ``min``, ``max``, ``approx_unique``,
``avg``, ``std``, ``q25``, ``q50``, ``q75``, ``count``, ``null_percentage``).
"""


__all__ = [
    "EXPLAIN_WRAPPER",
    "AUTO_LIMIT_WRAPPER",
    "PRESET_SAMPLE_ROWS",
    "PRESET_ROW_COUNT",
    "PRESET_COLUMN_STATS",
]
