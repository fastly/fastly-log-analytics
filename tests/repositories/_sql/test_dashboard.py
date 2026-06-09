"""Template-render tests for `backend.repositories._sql.dashboard`.

Phase 5a — verifies the format-template structure (no DuckDB needed). Each
template gets a render test that checks the rendered string contains the
expected fragments, plus a placeholder-set pin so future edits surface
unintended placeholder additions/removals.
"""

from __future__ import annotations

from backend.repositories._sql import dashboard as SQL


def _placeholders(template: str) -> list[str]:
    """Extract the ``{name}``-style format placeholders from ``template``."""
    return sorted({p.split("}")[0] for p in template.split("{")[1:] if "}" in p})


# ── VIRTUAL_FIELD_EXPLODED_TOP_N ──────────────────────────────────────────────


def test_virtual_field_exploded_top_n_renders_with_all_inputs():
    rendered = SQL.VIRTUAL_FIELD_EXPLODED_TOP_N.format(
        backing_col="waf_sig",
        table_name='"logs_xyz"',
        where_clause="1=1",
        requests_metric="COUNT(*)",
    )
    assert 'unnest(string_split("waf_sig", \',\'))' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "WITH split_data AS" in rendered
    assert "CROSS JOIN total_count" in rendered
    assert "LIMIT 10" in rendered


def test_virtual_field_exploded_top_n_placeholders_pinned():
    assert _placeholders(SQL.VIRTUAL_FIELD_EXPLODED_TOP_N) == sorted([
        "backing_col",
        "table_name",
        "where_clause",
        "requests_metric",
    ])


# ── CONN_REQUESTS_BUCKET ──────────────────────────────────────────────────────


def test_conn_requests_bucket_renders_with_all_inputs():
    rendered = SQL.CONN_REQUESTS_BUCKET.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
    )
    # Bucket labels must use en-dash (U+2013) — the frontend matches the exact strings.
    assert "'2–5'" in rendered
    assert "'6–20'" in rendered
    assert "'21+'" in rendered
    assert "ORDER BY MIN(\"conn_requests\")" in rendered
    assert 'FROM "logs_xyz"' in rendered


def test_conn_requests_bucket_placeholders_pinned():
    assert _placeholders(SQL.CONN_REQUESTS_BUCKET) == sorted([
        "requests_metric",
        "table_name",
        "where_clause",
    ])


# ── TIME_SERIES ───────────────────────────────────────────────────────────────


def test_time_series_renders_without_extra_where():
    rendered = SQL.TIME_SERIES.format(
        time_bucket_select="time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
        value_expr="COUNT(*)",
        table_name='"logs_xyz"',
        extra_where="",
        where_clause="status = 200",
    )
    assert "time_bucket(INTERVAL '1 minute', timestamp)" in rendered
    assert "COUNT(*) AS value" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "WHERE timestamp IS NOT NULL AND status = 200" in rendered
    assert "GROUP BY 1 ORDER BY 1" in rendered


def test_time_series_renders_with_extra_where_for_latency():
    rendered = SQL.TIME_SERIES.format(
        time_bucket_select="time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
        value_expr="PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) / 1000.0",
        table_name='"logs_xyz"',
        extra_where=' AND "elapsed" IS NOT NULL',
        where_clause="1=1",
    )
    # Extra-where injects between the timestamp gate and the main WHERE clause.
    assert 'WHERE timestamp IS NOT NULL AND "elapsed" IS NOT NULL AND 1=1' in rendered


def test_time_series_placeholders_pinned():
    assert _placeholders(SQL.TIME_SERIES) == sorted([
        "time_bucket_select",
        "value_expr",
        "table_name",
        "extra_where",
        "where_clause",
    ])


# ── MAP_DATA_BY_COUNTRY ───────────────────────────────────────────────────────


def test_map_data_by_country_renders():
    rendered = SQL.MAP_DATA_BY_COUNTRY.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
    )
    assert 'SELECT "country" AS country' in rendered
    assert "COUNT(*) AS count" in rendered
    assert 'WHERE "country" IS NOT NULL AND 1=1' in rendered
    assert "GROUP BY 1" in rendered


def test_map_data_by_country_placeholders_pinned():
    assert _placeholders(SQL.MAP_DATA_BY_COUNTRY) == sorted([
        "requests_metric",
        "table_name",
        "where_clause",
    ])


# ── FIELD_VALUES_BOT_UA ───────────────────────────────────────────────────────


def test_field_values_bot_ua_renders_with_filter():
    rendered = SQL.FIELD_VALUES_BOT_UA.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        ua_filter="AND regexp_matches(ua, 'bot|crawl')",
    )
    assert "SELECT ua, COUNT(*) AS cnt" in rendered
    assert "WHERE 1=1 AND ua IS NOT NULL AND regexp_matches(ua, 'bot|crawl')" in rendered
    assert "GROUP BY ua" in rendered
    assert "ORDER BY cnt DESC" in rendered
    assert "LIMIT 5000" in rendered


def test_field_values_bot_ua_renders_without_filter():
    rendered = SQL.FIELD_VALUES_BOT_UA.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        ua_filter="",
    )
    assert "WHERE 1=1 AND ua IS NOT NULL " in rendered


def test_field_values_bot_ua_placeholders_pinned():
    assert _placeholders(SQL.FIELD_VALUES_BOT_UA) == sorted([
        "requests_metric",
        "table_name",
        "where_clause",
        "ua_filter",
    ])


# ── FIELD_VALUES_VIRTUAL_SIGNALS ──────────────────────────────────────────────


def test_field_values_virtual_signals_renders_with_search():
    rendered = SQL.FIELD_VALUES_VIRTUAL_SIGNALS.format(
        requests_metric="COUNT(*)",
        backing_col="waf_sig",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond="AND trim(signal) ILIKE ?",
        limit=20,
    )
    assert 'unnest(string_split("waf_sig", \',\'))' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "WHERE trim(signal) != '' AND trim(signal) ILIKE ?" in rendered
    assert "LIMIT 20" in rendered


def test_field_values_virtual_signals_renders_without_search():
    rendered = SQL.FIELD_VALUES_VIRTUAL_SIGNALS.format(
        requests_metric="COUNT(*)",
        backing_col="edge_score_reason",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond="",
        limit=10,
    )
    assert 'unnest(string_split("edge_score_reason", \',\'))' in rendered
    assert "WHERE trim(signal) != '' " in rendered
    assert "LIMIT 10" in rendered


def test_field_values_virtual_signals_placeholders_pinned():
    assert _placeholders(SQL.FIELD_VALUES_VIRTUAL_SIGNALS) == sorted([
        "requests_metric",
        "backing_col",
        "table_name",
        "where_clause",
        "search_cond",
        "limit",
    ])


# ── FIELD_VALUES_NATIVE_COLUMN ────────────────────────────────────────────────


def test_field_values_native_column_renders_with_search():
    rendered = SQL.FIELD_VALUES_NATIVE_COLUMN.format(
        clean_field="country",
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond='AND CAST("country" AS VARCHAR) ILIKE ?',
        limit=50,
    )
    assert 'SELECT "country" AS value' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert 'WHERE 1=1 AND CAST("country" AS VARCHAR) ILIKE ?' in rendered
    assert "GROUP BY 1 ORDER BY 2 DESC LIMIT 50" in rendered


def test_field_values_native_column_renders_without_search():
    rendered = SQL.FIELD_VALUES_NATIVE_COLUMN.format(
        clean_field="asn",
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond="",
        limit=10,
    )
    assert 'SELECT "asn" AS value' in rendered
    assert "WHERE 1=1 " in rendered
    assert "LIMIT 10" in rendered


def test_field_values_native_column_placeholders_pinned():
    assert _placeholders(SQL.FIELD_VALUES_NATIVE_COLUMN) == sorted([
        "clean_field",
        "requests_metric",
        "table_name",
        "where_clause",
        "search_cond",
        "limit",
    ])


# ── Module-level invariants ───────────────────────────────────────────────────


def test_all_templates_exported():
    """Each template constant must appear in ``__all__`` so the renaming /
    deletion of a template surfaces as an import error in callers."""
    assert set(SQL.__all__) == {
        "VIRTUAL_FIELD_EXPLODED_TOP_N",
        "CONN_REQUESTS_BUCKET",
        "TIME_SERIES",
        "MAP_DATA_BY_COUNTRY",
        "FIELD_VALUES_BOT_UA",
        "FIELD_VALUES_VIRTUAL_SIGNALS",
        "FIELD_VALUES_NATIVE_COLUMN",
    }
