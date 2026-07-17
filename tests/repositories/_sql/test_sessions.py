"""Template-render tests for `backend.repositories._sql.sessions`.

Phase 5a — verifies the format-template structure for the sessions
CTE pipeline (no DuckDB required for string-level checks).
"""

from __future__ import annotations

from backend.repositories._sql import sessions as SQL


def _placeholders(template: str) -> list[str]:
    return sorted(p.split("}")[0] for p in template.split("{")[1:] if "}" in p)


# ── SESSIONS_CTE_PIPELINE ─────────────────────────────────────────────────────


def test_sessions_cte_pipeline_renders_with_all_inputs():
    rendered = SQL.SESSIONS_CTE_PIPELINE.format(
        group_key='"ip", "ja4"',
        ua_proj=', "ua"',
        status_proj=', "status"',
        resp_bytes_proj=', "resp_bytes"',
        rtt_proj=', "tcp_rtt"',
        asn_proj=', "asn"',
        country_proj=', "country"',
        url_proj=', "url"',
        edge_proj=', "edge"',
        edge_sid_proj=', "edge_sid"',
        cmcd_sid_proj=', "cmcd_sid"',
        table_name='"logs_xyz"',
        where_clause="timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)",
        part_key='"ip", "ja4"',
        extra_aggs=', SUM("resp_bytes") AS total_bytes, MAX("edge_sid") AS edge_sid',
    )
    # Five CTE stages must be present.
    assert "WITH base AS" in rendered
    assert "gaps AS" in rendered
    assert "marks AS" in rendered
    assert "sessions_raw AS" in rendered
    assert "sessions_agg AS" in rendered
    # Window-function shape pinned.
    assert "LAG(ts) OVER (PARTITION BY" in rendered
    assert "INTERVAL 30 MINUTES" in rendered
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in rendered
    # User-input fields are bound through ? params, NOT formatted in.
    assert "CAST(? AS TIMESTAMPTZ)" in rendered
    # Optional projections + aggregates substituted.
    assert '"ja4"' in rendered
    assert '"ua"' in rendered
    assert '"edge_sid"' in rendered
    assert '"cmcd_sid"' in rendered
    assert 'SUM("resp_bytes") AS total_bytes' in rendered
    assert 'MAX("edge_sid") AS edge_sid' in rendered
    # Table identifier substituted.
    assert 'FROM "logs_xyz"' in rendered


def test_sessions_cte_pipeline_renders_with_empty_optional_projections():
    """When optional columns are absent, projections collapse to empty
    strings — the CTE must still be valid SQL shape."""
    rendered = SQL.SESSIONS_CTE_PIPELINE.format(
        group_key='"ip"',
        ua_proj="",
        status_proj="",
        resp_bytes_proj="",
        rtt_proj="",
        asn_proj="",
        country_proj="",
        url_proj="",
        edge_proj="",
        edge_sid_proj="",
        cmcd_sid_proj="",
        table_name='"logs_xyz"',
        where_clause="1=1",
        part_key='"ip"',
        extra_aggs="",
    )
    assert "WITH base AS" in rendered
    assert 'PARTITION BY "ip"' in rendered
    # No ja4 / ua / status / edge_sid leakage when those columns are absent.
    assert '"ja4"' not in rendered
    assert '"ua"' not in rendered
    assert '"status"' not in rendered
    assert '"edge_sid"' not in rendered


def test_sessions_cte_pipeline_pins_all_expected_placeholders():
    """Pin the set of substitution names so accidental new placeholders
    in the template raise an immediate test failure."""
    assert set(_placeholders(SQL.SESSIONS_CTE_PIPELINE)) == {
        "group_key",  # used in base SELECT, sessions_agg SELECT, GROUP BY
        "ua_proj",
        "status_proj",
        "resp_bytes_proj",
        "rtt_proj",
        "asn_proj",
        "country_proj",
        "url_proj",
        "edge_proj",
        "edge_sid_proj",
        "cmcd_sid_proj",
        "table_name",
        "where_clause",
        "part_key",  # used in gaps + sessions_raw window functions
        "extra_aggs",
    }
    # And pin reuse counts so a stage drop / accidental duplication is caught.
    placeholders = _placeholders(SQL.SESSIONS_CTE_PIPELINE)
    assert placeholders.count("group_key") == 3
    assert placeholders.count("part_key") == 2


# ── SESSIONS_PAGE_SELECT ─────────────────────────────────────────────────────


def test_sessions_page_select_renders_with_all_inputs():
    rendered = SQL.SESSIONS_PAGE_SELECT.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count, NULL AS sid)",
        flag_expr="(req_count >= 1000)",
        flagged_filter="WHERE flagged = true",
        sort_by="session_start",
        sort_dir="DESC",
        limit=50,
        offset=100,
    )
    assert "WITH sessions_agg AS" in rendered
    # Template wraps the flag expr in parens to keep precedence safe.
    assert "((req_count >= 1000)) AS flagged" in rendered
    assert "FROM sessions_agg" in rendered
    assert "WHERE flagged = true" in rendered
    assert "ORDER BY session_start DESC" in rendered
    assert "LIMIT 50 OFFSET 100" in rendered


def test_sessions_page_select_handles_empty_flagged_filter():
    """When ``flagged_only`` is False, ``flagged_filter`` is an empty
    string and the ORDER BY must still render correctly."""
    rendered = SQL.SESSIONS_PAGE_SELECT.format(
        cte_prefix="",
        flag_expr="(req_count >= 1000)",
        flagged_filter="",
        sort_by="req_count",
        sort_dir="ASC",
        limit=20,
        offset=0,
    )
    assert "ORDER BY req_count ASC" in rendered
    assert "LIMIT 20 OFFSET 0" in rendered
    # No spurious WHERE clause when flagged_filter is empty.
    assert "WHERE flagged" not in rendered


def test_sessions_page_select_pins_all_expected_placeholders():
    assert _placeholders(SQL.SESSIONS_PAGE_SELECT) == sorted(
        [
            "cte_prefix",
            "flag_expr",
            "flagged_filter",
            "sort_by",
            "sort_dir",
            "limit",
            "offset",
        ]
    )


# ── SESSIONS_COUNT_WRAPPER ───────────────────────────────────────────────────


def test_sessions_count_wrapper_renders_with_all_inputs():
    rendered = SQL.SESSIONS_COUNT_WRAPPER.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count)",
        flag_expr="(req_count >= 1000) OR ((reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0)",
        flagged_filter="WHERE flagged = true",
    )
    assert "WITH sessions_agg AS" in rendered
    assert "SELECT COUNT(*) FROM (SELECT" in rendered
    assert "AS flagged FROM sessions_agg) sub" in rendered
    assert "WHERE flagged = true" in rendered
    assert "(reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0" in rendered


def test_sessions_count_wrapper_pins_all_expected_placeholders():
    assert _placeholders(SQL.SESSIONS_COUNT_WRAPPER) == sorted(
        [
            "cte_prefix",
            "flag_expr",
            "flagged_filter",
        ]
    )
