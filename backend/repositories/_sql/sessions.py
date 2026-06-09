"""SQL templates for `backend.repositories.sessions`.

Phase 5a extraction. See ``pending-docs/sql_ownership_audit.md`` for the
mechanical recipe and ``backend/repositories/_sql/__init__.py`` for the
ownership policy.

The sessions repository builds a multi-stage CTE pipeline that:

1. Filters the raw log table to the requested window + filters (``base``).
2. Computes inter-row time gaps per IP/JA4 partition (``gaps``).
3. Marks the start of every new session when the gap > 30 minutes
   (``marks``).
4. Assigns a session id via a running sum (``sessions_raw``).
5. Aggregates per session to produce the final session-level rows
   (``sessions_agg``).

All template placeholders are trusted-identifier substitutions only
(table name, allowlisted column projections, validated sort column).
User-supplied window bounds and filter values are bound through DuckDB
``?`` parameters via ``runner.execute_with_retry(...)``.
"""

from __future__ import annotations

# ── Sessions CTE pipeline ────────────────────────────────────────────────────

SESSIONS_CTE_PIPELINE = """
    WITH base AS (
        SELECT {group_key}
               {ua_proj}
               , timestamp AS ts
               {status_proj}
               {resp_bytes_proj}
               {rtt_proj}
               {asn_proj}
               {country_proj}
               {url_proj}
               {edge_proj}
               {edge_sid_proj}
        FROM {table_name}
        WHERE {where_clause} AND timestamp IS NOT NULL
    ),
    gaps AS (
        SELECT *,
               ts - LAG(ts) OVER (PARTITION BY {part_key} ORDER BY ts) AS gap
        FROM base
    ),
    marks AS (
        SELECT *,
               CASE WHEN gap IS NULL OR gap > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new
        FROM gaps
    ),
    sessions_raw AS (
        SELECT *,
               SUM(is_new) OVER (PARTITION BY {part_key} ORDER BY ts
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sid
        FROM marks
    ),
    sessions_agg AS (
        SELECT {group_key},
               MIN(ts) AS session_start,
               MAX(ts) AS session_end,
               COUNT(*) AS req_count
               {extra_aggs}
               , sid
        FROM sessions_raw
        GROUP BY {group_key}, sid
    )
"""
"""Five-stage CTE pipeline that materialises per-session aggregates.

Inputs (all trusted-identifier substitutions):
- ``{group_key}`` — quoted partition columns: ``"ip"`` or ``"ip", "ja4"``.
- ``{ua_proj}`` — empty string or ``, "ua"`` (column projection in ``base``).
- ``{status_proj}`` — empty string or ``, "status"``.
- ``{resp_bytes_proj}`` — empty string or ``, "resp_bytes"``.
- ``{rtt_proj}`` — empty string or ``, "tcp_rtt"``.
- ``{asn_proj}`` — empty string or ``, "asn"``.
- ``{country_proj}`` — empty string or ``, "country"``.
- ``{url_proj}`` — empty string or ``, "url"``.
- ``{edge_proj}`` — empty string or ``, "edge"``.
- ``{edge_sid_proj}`` — empty string or ``, "edge_sid"`` (Fastly cookie
  session id; present only after the session_scoring orchestrator has
  provisioned the field — see
  ``backend/provision/session_scoring_orchestrator.py``).
- ``{table_name}`` — output of ``_safe_table(src["name"])``.
- ``{where_clause}`` — output of ``build_where_clause(...)`` (uses ``?`` for
  user values; the caller binds those via the ``params`` arg, not here).
- ``{part_key}`` — same value as ``{group_key}`` (the partition key for the
  window functions; kept distinct in case callers ever diverge them).
- ``{extra_aggs}`` — pre-built per-column aggregate clauses (each begins
  with ``, ``) for the optional columns above.

Output (one row per ``(group_key, sid)``):
- ``{group_key}`` columns (``ip``, optionally ``ja4``)
- ``session_start`` (TIMESTAMPTZ)
- ``session_end`` (TIMESTAMPTZ)
- ``req_count`` (BIGINT)
- the columns produced by ``{extra_aggs}`` (asn, country, reqs_4xx,
  reqs_5xx, total_bytes, median_rtt_ms, ua, unique_urls, edge_count,
  shield_count — presence depends on table schema)
- ``sid`` (BIGINT — running session id; the caller does not select it)

This template is intended to be combined with a downstream query
(``SESSIONS_PAGE_SELECT`` or ``SESSIONS_COUNT_WRAPPER``) via string
concatenation in the repository — both consumers reference the
``sessions_agg`` CTE produced here.
"""


SESSIONS_PAGE_SELECT = """
    {cte_prefix}
    SELECT *, ({flag_expr}) AS flagged
    FROM sessions_agg
    {flagged_filter}
    ORDER BY {sort_by} {sort_dir}
    LIMIT {limit} OFFSET {offset}
"""
"""Page of sessions with the flagged-suspect predicate applied.

Inputs (all trusted-identifier substitutions; user values are bound
through DuckDB ``?`` params in ``{cte_prefix}``'s WHERE clause):
- ``{cte_prefix}`` — rendered ``SESSIONS_CTE_PIPELINE``.
- ``{flag_expr}`` — caller-built boolean (e.g.
  ``(req_count >= 1000) OR (...)``) — values are inline integer/float
  literals validated by the repository layer.
- ``{flagged_filter}`` — empty string or ``WHERE flagged = true``.
- ``{sort_by}`` — column name from a hard-coded allowlist (validated in
  the repository).
- ``{sort_dir}`` — ``ASC`` or ``DESC`` (validated upstream by the router).
- ``{limit}`` — integer (validated upstream by the router pagination guard).
- ``{offset}`` — integer (computed by ``calc_offset(page, limit)``).

Output (one row per session): all columns produced by the
``sessions_agg`` CTE plus a synthesised ``flagged`` BOOLEAN.
"""


SESSIONS_COUNT_WRAPPER = """
    {cte_prefix}
    SELECT COUNT(*) FROM (SELECT ({flag_expr}) AS flagged FROM sessions_agg) sub
    {flagged_filter}
"""
"""Total session count (with optional flagged-only filter applied).

Used by the repository only when the page request hits past the last
page (the cheap ``len(rows)`` fast path returns 0; we then need the true
total to render the paginator).

Inputs:
- ``{cte_prefix}`` — rendered ``SESSIONS_CTE_PIPELINE``.
- ``{flag_expr}`` — same flag expression as ``SESSIONS_PAGE_SELECT``.
- ``{flagged_filter}`` — empty string or ``WHERE flagged = true``.

Output (one row):
- column 0: BIGINT — total session count after filtering.
"""


__all__ = [
    "SESSIONS_CTE_PIPELINE",
    "SESSIONS_PAGE_SELECT",
    "SESSIONS_COUNT_WRAPPER",
]
