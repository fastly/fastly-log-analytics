"""Template-render tests for `backend.repositories._sql.base`.

Phase 5a — verifies the format-template structure for the shared
QueryRunner SQL fragments (no DuckDB needed for these string checks).

Per-template tests pin two things:

- the rendered string contains the expected fragments (so a typo in
  the template is caught even when the runtime test happens to mask it);
- the set of ``{...}`` placeholders matches the caller in ``_base.py``
  (so an accidental new placeholder fails this test immediately rather
  than blowing up at first runtime call with a ``KeyError``).
"""

from __future__ import annotations

from backend.repositories._sql import base as SQL


def _placeholders(template: str) -> list[str]:
    return sorted(p.split("}")[0] for p in template.split("{")[1:] if "}" in p)


# ── CANONICAL_METRICS dict ───────────────────────────────────────────────────


def test_canonical_metrics_required_keys_present():
    """Pin the metric key set so a rename can't silently break the
    dashboard repository (which looks these up by string key)."""
    assert set(SQL.CANONICAL_METRICS) == {
        "hit_rate",
        "requests",
        "avg_ttfb",
        "p95_ttfb",
        "5xx_rate",
        "4xx_rate",
        "avg_resp_bytes",
        "total_resp_bytes",
        "throughput",
        "req_size",
        "ttfb_ms",
    }


def test_canonical_metrics_requests_is_count_star():
    """``requests`` is the simplest expression and the one used as
    ``requests_metric`` across many dashboard templates."""
    assert SQL.CANONICAL_METRICS["requests"] == "COUNT(*)"


def test_canonical_metrics_hit_rate_renders_with_cache_col():
    rendered = SQL.CANONICAL_METRICS["hit_rate"].format(cache_col='"cache"')
    assert 'WHERE "cache" IN' in rendered
    assert "'HIT', 'HIT-STALE'" in rendered


def test_canonical_metrics_throughput_preserves_double_percent_literal():
    """The throughput template carries a literal ``HIT%%`` in the
    ILIKE pattern, preserved byte-for-byte from the historical inline
    definition. DuckDB ILIKE treats ``%%`` as two wildcards (each
    matching the empty string), so the match semantics equal ``HIT%``
    — but the bytes that reach DuckDB are ``%%``. Pinning this here so
    a "helpful" refactor that collapses to a single ``%`` can't
    silently change what hits the engine."""
    assert "%%" in SQL.CANONICAL_METRICS["throughput"]
    rendered = SQL.CANONICAL_METRICS["throughput"].format(
        cache_col='"cache"',
        elapsed_col='"elapsed"',
        resp_bytes_col='"resp_bytes"',
    )
    # str.format does NOT special-case ``%%`` — it survives intact.
    assert "ILIKE 'HIT%%'" in rendered


# ── TS_ROLLUP_METRIC_SQL / LIVE_METRIC_SQL_FROM_RAW dicts ────────────────────


def test_ts_rollup_and_live_metric_keys_match():
    """The rollup-served metric set must match the raw-row counterpart
    set exactly — otherwise an active-hour split where one side has the
    metric and the other doesn't would silently drop a chart band."""
    assert (
        set(SQL.TS_ROLLUP_METRIC_SQL)
        == set(SQL.LIVE_METRIC_SQL_FROM_RAW)
        == {
            "requests",
            "5xx",
            "4xx",
            "hit_rate",
        }
    )


def test_ts_rollup_metric_sql_uses_rollup_columns():
    """Rollup expressions reference pre-aggregated columns
    (``requests``, ``status_5xx``, ``status_4xx``, ``hits``) — they
    SUM, not COUNT."""
    assert SQL.TS_ROLLUP_METRIC_SQL["requests"] == "CAST(SUM(requests) AS BIGINT)"
    assert "SUM(status_5xx)" in SQL.TS_ROLLUP_METRIC_SQL["5xx"]
    assert "SUM(status_4xx)" in SQL.TS_ROLLUP_METRIC_SQL["4xx"]
    assert "SUM(hits)" in SQL.TS_ROLLUP_METRIC_SQL["hit_rate"]


def test_live_metric_sql_from_raw_uses_count_filter():
    """Live (raw-row) expressions must use ``COUNT(*) FILTER`` to
    match the per-row semantics the rollup writer originally applied."""
    assert SQL.LIVE_METRIC_SQL_FROM_RAW["requests"] == "COUNT(*)"
    assert "COUNT(*) FILTER (WHERE status >= 500)" in SQL.LIVE_METRIC_SQL_FROM_RAW["5xx"]
    assert "COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499)" in SQL.LIVE_METRIC_SQL_FROM_RAW["4xx"]
    assert "COUNT(*) FILTER (WHERE cache IN" in SQL.LIVE_METRIC_SQL_FROM_RAW["hit_rate"]


# ── TOP_N_ROLLUP_AGGREGATE ───────────────────────────────────────────────────


def test_top_n_rollup_aggregate_renders_with_branches():
    rendered = SQL.TOP_N_ROLLUP_AGGREGATE.format(
        branches_union_all="SELECT 1 AS field, 'x' AS value, 1 AS count "
        "UNION ALL SELECT 2 AS field, 'y' AS value, 2 AS count"
    )
    assert "SELECT field, value, SUM(count) AS c" in rendered
    assert "GROUP BY field, value" in rendered
    assert "UNION ALL" in rendered


def test_top_n_rollup_aggregate_pins_placeholders():
    assert _placeholders(SQL.TOP_N_ROLLUP_AGGREGATE) == ["branches_union_all"]


# ── TS_LIVE_CLAUSE ───────────────────────────────────────────────────────────


def test_ts_live_clause_renders_with_all_inputs():
    rendered = SQL.TS_LIVE_CLAUSE.format(
        interval="1 minute",
        metric_sql="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        live_st_iso="2026-06-09T15:00:00",
        live_et_iso="2026-06-09T16:00:00",
    )
    # Bucket expression uses interval literal directly (validated upstream
    # via ``_TS_ROLLUP_INTERVALS`` allowlist).
    assert "time_bucket(INTERVAL '1 minute', timestamp) AS out_bucket" in rendered
    # Metric expression substituted.
    assert "COUNT(*) AS value" in rendered
    # Table identifier substituted.
    assert 'FROM "logs_xyz"' in rendered
    # Window bounds rendered with explicit ``+00:00`` UTC suffix appended
    # outside the ISO placeholder.
    assert "TIMESTAMPTZ '2026-06-09T15:00:00+00:00'" in rendered
    assert "TIMESTAMPTZ '2026-06-09T16:00:00+00:00'" in rendered
    # Half-open semantics on the upper bound.
    assert "timestamp <  TIMESTAMPTZ" in rendered
    # GROUP BY 1 buckets per out_bucket.
    assert "GROUP BY 1" in rendered


def test_ts_live_clause_pins_placeholders():
    assert _placeholders(SQL.TS_LIVE_CLAUSE) == sorted(
        [
            "interval",
            "metric_sql",
            "table_name",
            "where_clause",
            "live_st_iso",
            "live_et_iso",
        ]
    )


# ── TS_OUTER_WRAPPER ─────────────────────────────────────────────────────────


def test_ts_outer_wrapper_renders_with_unioned_clauses():
    rendered = SQL.TS_OUTER_WRAPPER.format(
        unioned_clauses="(SELECT 1 AS out_bucket, 2 AS value) UNION ALL (SELECT 3, 4)"
    )
    assert "SELECT out_bucket, value FROM" in rendered
    # NULL filter prevents empty-bucket rows from poisoning the chart.
    assert "WHERE out_bucket IS NOT NULL" in rendered
    assert "ORDER BY 1" in rendered


def test_ts_outer_wrapper_pins_placeholders():
    assert _placeholders(SQL.TS_OUTER_WRAPPER) == ["unioned_clauses"]


# ── TOP_N_BATCH_PER_FIELD ────────────────────────────────────────────────────


def test_top_n_batch_per_field_renders_with_all_inputs():
    rendered = SQL.TOP_N_BATCH_PER_FIELD.format(
        field="country",
        select_val='"country"',
        table_name='"logs_xyz"',
        where_filter='"country" IS NOT NULL AND "country" != \'\'',
        limit=10,
    )
    # The field name is inlined as a string literal so result rows can be
    # demuxed in Python — this is intentional, NOT a parameterisation bug.
    assert "SELECT 'country' as field" in rendered
    # Column projection substituted.
    assert '"country" as value' in rendered
    # Grouping/order/limit shape preserved.
    assert "GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10" in rendered
    # Subquery is wrapped in parens so the caller can UNION ALL it
    # directly with sibling per-field subqueries.
    assert rendered.strip().startswith("(SELECT")
    assert rendered.strip().endswith(")")


def test_top_n_batch_per_field_int_aggregate_select_val():
    """Fields like ``ttl`` / ``age`` use a CAST/ROUND wrapper to collapse
    floating-point jitter at ingest into integer-rounded buckets."""
    rendered = SQL.TOP_N_BATCH_PER_FIELD.format(
        field="ttl",
        select_val='CAST(CAST(ROUND("ttl") AS INTEGER) AS VARCHAR)',
        table_name='"logs_xyz"',
        where_filter='"ttl" IS NOT NULL',
        limit=10,
    )
    assert 'CAST(CAST(ROUND("ttl") AS INTEGER) AS VARCHAR) as value' in rendered


def test_top_n_batch_per_field_pins_placeholders():
    assert _placeholders(SQL.TOP_N_BATCH_PER_FIELD) == sorted(
        [
            "field",
            "select_val",
            "table_name",
            "where_filter",
            "limit",
        ]
    )


# ── Module-level placeholder pin ─────────────────────────────────────────────


def test_module_exports_pin():
    """Lock the public surface so an accidental rename in
    ``_sql/base.py`` shows up as a test failure here rather than as a
    runtime ImportError in ``_base.py``."""
    assert set(SQL.__all__) == {
        "CANONICAL_METRICS",
        "TS_ROLLUP_METRIC_SQL",
        "LIVE_METRIC_SQL_FROM_RAW",
        "TOP_N_ROLLUP_AGGREGATE",
        "TS_LIVE_CLAUSE",
        "TS_OUTER_WRAPPER",
        "TOP_N_BATCH_PER_FIELD",
    }
