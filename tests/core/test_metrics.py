"""Tests for backend.core.metrics.

``get_metric_sql`` is the single source of truth for every aggregation
expression any analytical endpoint runs. A typo here lands on every
chart in the product, so we pin both the SQL shape and (for the
performance-critical cases) the optimised WHERE-clause path.
"""

from __future__ import annotations

import duckdb
import pytest

from backend.core.metrics import METRIC_DEFINITIONS, get_metric_sql
from backend.models.metrics import MetricType

# ── Smoke: every declared metric has an SQL definition ──────────────────────


def test_every_metric_type_has_a_definition_or_is_specific_status():
    """Every MetricType either appears in METRIC_DEFINITIONS or is
    ``specific_status`` (which is computed dynamically from status codes
    and never lives in the dict)."""
    for m in MetricType:
        assert m.value in METRIC_DEFINITIONS or m == MetricType.SPECIFIC_STATUS


# ── get_metric_sql: bare aggregation form (no table_name) ───────────────────


def test_returns_aggregation_for_simple_metric():
    """Without ``table_name`` we get just the aggregation expression
    (caller wraps it in their own SELECT)."""
    sql = get_metric_sql(MetricType.REQUESTS.value)
    assert sql == "count(*)"


def test_returns_aggregation_for_5xx_rate():
    sql = get_metric_sql("5xx_rate")
    assert "100.0" in sql  # percentage
    assert "NULLIF(count(*), 0)" in sql  # division-by-zero guard


def test_unknown_metric_falls_back_to_count():
    """Defensive default — an unknown metric returns ``count(*)`` rather
    than ``None`` so callers never see a NULL SQL fragment."""
    sql = get_metric_sql("totally_made_up_metric")
    assert sql == "count(*)"


# ── get_metric_sql: optimised SELECT form (with table_name) ─────────────────


def test_5xx_uses_where_clause_optimization():
    """The ``5xx`` count is fast: ``WHERE status >= 500`` lets DuckDB
    skip rows entirely rather than running ``sum(CASE WHEN ...)``."""
    sql = get_metric_sql(MetricType.ERRORS_5XX.value, table_name="logs")
    assert "WHERE status >= 500" in sql
    assert "CASE WHEN" not in sql  # confirms the WHERE optimization
    assert "FROM logs" in sql


def test_4xx_uses_where_clause_optimization():
    sql = get_metric_sql(MetricType.ERRORS_4XX.value, table_name="logs")
    assert "WHERE status BETWEEN 400 AND 499" in sql
    assert "CASE WHEN" not in sql


def test_ttfb_filters_on_not_null():
    """``ottfb`` is sparsely populated — a WHERE ottfb IS NOT NULL
    avoids forcing PERCENTILE_CONT to scan NULL rows."""
    sql = get_metric_sql(MetricType.TTFB.value, table_name="logs")
    assert "ottfb IS NOT NULL" in sql
    assert "FROM logs" in sql


def test_requests_with_table_wraps_in_select():
    sql = get_metric_sql(MetricType.REQUESTS.value, table_name="logs_svc")
    assert sql.startswith("SELECT ")
    assert "FROM logs_svc" in sql
    assert "count(*)" in sql


# ── specific_status: dynamic status-code filtering ──────────────────────────


def test_specific_status_renders_status_list_into_select():
    sql = get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[404, 403], table_name="logs")
    # The renderer leaves an extra space between ``1=1`` and ``AND`` — harmless,
    # but pinned so a refactor that "tidies" it is forced through this test.
    assert "WHERE 1=1" in sql
    assert "status IN (404, 403)" in sql
    assert "FROM logs" in sql


def test_specific_status_rate_uses_aggregation_form():
    """``specific_status_rate`` is a percentage — must use the
    sum/count form, not the WHERE optimisation."""
    sql = get_metric_sql("specific_status_rate", status_codes=[500])
    assert "sum(CASE WHEN 1=1  AND status IN (500) THEN 1 ELSE 0 END)" in sql
    assert "100.0" in sql
    assert "NULLIF(count(*), 0)" in sql


def test_specific_status_empty_codes_renders_no_filter():
    """Empty status_codes is a no-op — produces no IN-clause."""
    sql = get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[])
    assert "IN" not in sql


def test_specific_status_ignores_non_int_codes():
    """Hostile input safety: a non-int slipped into status_codes must
    not land in the SQL (e.g. as a string injection vector)."""
    sql = get_metric_sql(
        MetricType.SPECIFIC_STATUS.value,
        status_codes=[200, "drop table users", 404],  # type: ignore[list-item]
    )
    # Only the integers should make it through
    assert "200" in sql and "404" in sql
    assert "drop" not in sql.lower()


# ── End-to-end: generated SQL actually executes ─────────────────────────────


@pytest.fixture(scope="module")
def t():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs (status INT, elapsed BIGINT, ottfb DOUBLE, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    con.execute(
        "INSERT INTO logs VALUES (200, 100, 50.0, 'HIT', 1024, 200, 100), (500, 500, 80.0, 'MISS', 2048, 300, 100), (404, 30, NULL, 'PASS', 512, 100, 100)"
    )
    yield con
    con.close()


@pytest.mark.parametrize(
    "metric",
    [
        MetricType.REQUESTS.value,
        MetricType.ERRORS_5XX.value,
        MetricType.ERRORS_4XX.value,
        MetricType.HIT_RATE.value,
        MetricType.LATENCY_P95.value,
        MetricType.THROUGHPUT.value,
        MetricType.REQ_SIZE.value,
        MetricType.TTFB.value,
        "5xx_rate",
        "4xx_rate",
    ],
)
def test_generated_sql_executes_against_real_table(metric, t):
    """Every standard metric's generated SQL must execute against a
    real table without DuckDB raising. Catches silent operator/column
    drift, missing parens, etc."""
    sql = get_metric_sql(metric, table_name="logs")
    t.execute(sql).fetchone()


def test_specific_status_generated_sql_executes(t):
    sql = get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[200, 404], table_name="logs")
    rows = t.execute(sql).fetchone()
    assert rows[0] == 2  # we inserted one 200 and one 404
