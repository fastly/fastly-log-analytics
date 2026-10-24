"""Unit tests for the canonical metric-expression catalog.

Asserts the EXACT SQL string returned for every availability shape and for
each ``absent`` value the real call sites pass — this is a byte-for-byte
contract (the rollup parquet schema and the repository query SQL both depend
on these literals being unchanged).
"""

from __future__ import annotations

import gc
import json
import signal
import threading
import uuid
import weakref
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from backend import config
from backend import config as svcconfig
from backend.core import (
    memory_guard,
    metric_expressions,
    metric_snapshots,
    operational_metrics,
    request_metrics,
    request_telemetry,
    web_vitals_store,
)
from backend.core import request_context as rc
from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
from backend.core.metadata import slow_queries, usage_log, usage_log_db
from backend.core.metadata.usage_log import clear_usage_log
from backend.core.metrics import METRIC_DEFINITIONS, get_metric_sql
from backend.core.query_instrumentation import (
    InstrumentedDuckDBConnection,
    _InstrumentedRecordReader,
    _InstrumentedResult,
    _probe_duckdb_memory,
    _safe_weakref,
)
from backend.core.request_context import RequestContext, _enforce_service_access
from backend.main import app
from backend.models._base import MetricType
from backend.utils.date_utils import iso_z
from backend.utils.remote_access import TimeBounds
from backend.utils.telemetry import get_page_load_id
from backend.utils.time_window import resolve_window
from tests._helpers.config_fakes import fetch_metric_snapshot_history as _get_history
from tests.conftest import make_test_request_context


# ── Merged from tests/core/test_metric_expressions.py ──
class TestOriginLatencyUs:
    def test_both_columns_prefers_ottfb_with_ttfb_fallback(self) -> None:
        for cols in ({"ottfb", "ttfb"}, ["ottfb", "ttfb"], {"ottfb", "ttfb", "url"}):
            assert metric_expressions.origin_latency_us(cols) == 'COALESCE("ottfb", "ttfb" * 1000000.0)'

    def test_ottfb_only(self) -> None:
        assert metric_expressions.origin_latency_us({"ottfb"}) == '"ottfb"'

    def test_ttfb_only_scaled_to_us(self) -> None:
        assert metric_expressions.origin_latency_us({"ttfb"}) == '"ttfb" * 1000000.0'

    def test_neither_defaults_to_null_literal(self) -> None:
        # Default absent="NULL" — the repository seed's contract (always
        # valid SQL).
        assert metric_expressions.origin_latency_us(set()) == "NULL"
        assert metric_expressions.origin_latency_us({"url", "pop"}) == "NULL"

    def test_neither_with_absent_none_signals_skip(self) -> None:
        # The rollup writers pass absent=None to mean "skip this service".
        assert metric_expressions.origin_latency_us(set(), absent=None) is None
        assert metric_expressions.origin_latency_us({"url"}, absent=None) is None

    def test_present_branches_ignore_absent_value(self) -> None:
        # absent only affects the neither-present case.
        assert (
            metric_expressions.origin_latency_us({"ottfb", "ttfb"}, absent=None)
            == 'COALESCE("ottfb", "ttfb" * 1000000.0)'
        )
        assert metric_expressions.origin_latency_us({"ottfb"}, absent=None) == '"ottfb"'
        assert metric_expressions.origin_latency_us({"ttfb"}, absent=None) == '"ttfb" * 1000000.0'


class TestBaseSeedDelegation:
    """The repository seed must stay a byte-faithful delegate."""

    def test_origin_latency_us_expr_matches_catalog(self) -> None:
        from backend.repositories._base import origin_latency_us_expr

        for cols in (
            {"ottfb", "ttfb"},
            {"ottfb"},
            {"ttfb"},
            set(),
            {"url"},
        ):
            assert origin_latency_us_expr(cols) == metric_expressions.origin_latency_us(cols, absent="NULL")

        # Never returns None (always valid SQL).
        assert origin_latency_us_expr(set()) == "NULL"


class TestNullDefaultHelpersGoldenSql:
    """Each case pins the literal that a repository call site rendered before
    it was moved onto the helpers; a change here changes served SQL."""

    def test_hit_states_literal(self) -> None:
        assert metric_expressions.HIT_STATES == "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"

    def test_assets_column_defaults_present(self) -> None:
        cols = {"resp_header_content_encoding", "resp_bytes", "status", "ttl", "cache"}
        f = metric_expressions.column_or_default
        assert f(cols, "resp_header_content_encoding", "''") == "COALESCE(\"resp_header_content_encoding\", '')"
        assert f(cols, "resp_bytes", "0", cast="BIGINT") == 'COALESCE(CAST("resp_bytes" AS BIGINT), 0)'
        assert f(cols, "status", "200", cast="INTEGER") == 'COALESCE(CAST("status" AS INTEGER), 200)'
        assert f(cols, "ttl", "0.0", cast="DOUBLE") == 'COALESCE(CAST("ttl" AS DOUBLE), 0.0)'
        assert f(cols, "cache", "'MISS'") == "COALESCE(\"cache\", 'MISS')"

    def test_assets_column_defaults_absent(self) -> None:
        f = metric_expressions.column_or_default
        assert f(set(), "resp_header_content_encoding", "''") == "''"
        assert f(set(), "resp_bytes", "0", cast="BIGINT") == "0"
        assert f({"url"}, "status", "200", cast="INTEGER") == "200"
        assert f(set(), "ttl", "0.0", cast="DOUBLE") == "0.0"
        assert f(set(), "cache", "'MISS'") == "'MISS'"

    def test_performance_waterfall_exprs(self) -> None:
        c = metric_expressions.coalesce
        assert c("CAST(ottfb AS DOUBLE) / 1000.0", "0") == "COALESCE(CAST(ottfb AS DOUBLE) / 1000.0, 0)"
        assert c("CAST(ottlb AS DOUBLE) / 1000.0", "0") == "COALESCE(CAST(ottlb AS DOUBLE) / 1000.0, 0)"

    def test_sessions_sum_exprs(self) -> None:
        s = metric_expressions.sum_or_zero
        assert f"CAST({s('"resp_bytes"')} AS BIGINT)" == 'CAST(COALESCE(SUM("resp_bytes"), 0) AS BIGINT)'
        assert f"CAST({s('"tcp_rtt"', zero='0.0')} AS DOUBLE)" == 'CAST(COALESCE(SUM("tcp_rtt"), 0.0) AS DOUBLE)'

    def test_base_rollup_totals(self) -> None:
        s = metric_expressions.sum_or_zero
        assert s("requests") == "COALESCE(SUM(requests), 0)"
        assert s("num") == "COALESCE(SUM(num), 0)"

    def test_network_total_bytes(self) -> None:
        s = metric_expressions.sum_or_zero
        assert f"{s('resp_bytes')} AS total_bytes" == "COALESCE(SUM(resp_bytes), 0) AS total_bytes"
        assert f"{s('bytes')} AS total_bytes" == "COALESCE(SUM(bytes), 0) AS total_bytes"

    def test_value_live_tail_exprs(self) -> None:
        s = metric_expressions.sum_or_zero
        hit = metric_expressions.HIT_STATES
        assert (
            s("elapsed", zero="0.0", where="cache = 'MISS'")
            == "COALESCE(SUM(elapsed) FILTER (WHERE cache = 'MISS'), 0.0)"
        )
        assert (
            s("resp_bytes", where=f"cache IN {hit}")
            == "COALESCE(SUM(resp_bytes) FILTER (WHERE cache IN ('HIT', 'HIT-STALE', 'HIT-CLUSTER')), 0)"
        )
        assert s("resp_bytes") == "COALESCE(SUM(resp_bytes), 0)"
        assert (
            s("elapsed", zero="0.0", where=f"cache IN {hit}")
            == "COALESCE(SUM(elapsed) FILTER (WHERE cache IN ('HIT', 'HIT-STALE', 'HIT-CLUSTER')), 0.0)"
        )

    def test_usage_bytes_total(self) -> None:
        assert metric_expressions.coalesce("SUM(resp_bytes)", "0") == "COALESCE(SUM(resp_bytes), 0)"
        assert metric_expressions.coalesce("0", "0") == "COALESCE(0, 0)"


# ── Merged from tests/core/test_metric_snapshots.py ──
class _FakePgCursor:
    def __init__(self, rows=(), rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakePgConnection:
    def __init__(self, rows=(), rowcount=0):
        self.calls = []
        self._rows = rows
        self._rowcount = rowcount

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return _FakePgCursor(self._rows, self._rowcount)

    def close(self):
        pass


def test_record_and_get_global_metric_roundtrip():
    # Explicit timestamps because the second-resolution PK would otherwise
    # collide on two writes in the same second. Production sampler fires
    # every 60s so this is only a test-pace concern.
    now = datetime.now(UTC)
    metric_snapshots.record_snapshot("cpu_load_1m", 1.5, ts=iso_z(now - timedelta(seconds=10)))
    metric_snapshots.record_snapshot("cpu_load_1m", 2.0, ts=iso_z(now))
    out = _get_history("cpu_load_1m", since=now - timedelta(hours=1))
    assert len(out) == 2
    assert {r["value"] for r in out} == {1.5, 2.0}
    for row in out:
        assert "ts" in row and "value" in row


def test_per_service_and_per_task_filtering():
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 12.3, service_id="svc-a")
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 45.6, service_id="svc-b")
    metric_snapshots.record_snapshot("cron_duration_ms", 1000.0, service_id="svc-a", task="sync")
    metric_snapshots.record_snapshot("cron_duration_ms", 2000.0, service_id="svc-a", task="commit")

    since = datetime.now(UTC) - timedelta(hours=1)

    svc_a_pool = _get_history("pool_wait_p95_ms", since=since, service_id="svc-a")
    assert [r["value"] for r in svc_a_pool] == [12.3]

    svc_b_pool = _get_history("pool_wait_p95_ms", since=since, service_id="svc-b")
    assert [r["value"] for r in svc_b_pool] == [45.6]

    sync_dur = _get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")
    assert [r["value"] for r in sync_dur] == [1000.0]


def test_global_query_excludes_per_service_rows():
    """A global lookup without service_id must NOT return rows that have a
    service_id — otherwise the global CPU/disk panels would accidentally
    pull in per-service pool_wait samples."""
    metric_snapshots.record_snapshot("cpu_load_1m", 0.5)
    metric_snapshots.record_snapshot("cpu_load_1m", 99.0, service_id="svc-x")

    since = datetime.now(UTC) - timedelta(hours=1)
    out = _get_history("cpu_load_1m", since=since)
    assert [r["value"] for r in out] == [0.5]


def test_get_batch_keys_by_scope():
    metric_snapshots.record_snapshot("cpu_load_1m", 0.7)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 5.0, service_id="svc-a")
    metric_snapshots.record_snapshot("cron_duration_ms", 800.0, service_id="svc-a", task="sync")

    batch = metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1))
    assert "cpu_load_1m" in batch
    assert "pool_wait_p95_ms|svc-a" in batch
    assert "cron_duration_ms|svc-a|sync" in batch
    assert len(batch["cpu_load_1m"]) == 1
    assert batch["cpu_load_1m"][0]["value"] == 0.7


def test_since_clamp_excludes_old_rows():
    # Stamp a row in the distant past directly via the ts override.
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=10))
    metric_snapshots.record_snapshot("cpu_load_1m", 1.1, ts=old_ts)
    # And a fresh row.
    metric_snapshots.record_snapshot("cpu_load_1m", 2.2)

    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert [r["value"] for r in out] == [2.2]


def test_purge_old_drops_only_aged_rows():
    fresh_ts = iso_z(datetime.now(UTC) - timedelta(days=1))
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=60))
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0, ts=fresh_ts)
    metric_snapshots.record_snapshot("cpu_load_1m", 9.9, ts=old_ts)

    removed = metric_snapshots.purge_old(retention_days=30)
    assert removed == 1

    remaining = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(days=90))
    assert [r["value"] for r in remaining] == [1.0]


def test_purge_zero_or_negative_retention_is_noop():
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0)
    assert metric_snapshots.purge_old(retention_days=0) == 0
    assert metric_snapshots.purge_old(retention_days=-5) == 0
    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert len(out) == 1


def test_get_batch_returns_empty_before_first_write():
    """Reader path must not raise when no rows exist yet."""
    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert out == []
    batch = metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1))
    assert batch == {}


def test_idempotent_insert_overwrites_same_pk_per_service():
    """Replays of the same (metric, service_id, ts) for a per-service
    series should not stack up — the composite PK dedupes them."""
    ts = iso_z(datetime.now(UTC))
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 1.0, service_id="svc-a", ts=ts)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 2.0, service_id="svc-a", ts=ts)

    out = _get_history("pool_wait_p95_ms", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-a")
    assert len(out) == 1
    assert out[0]["value"] == 2.0


def test_postgres_backend_is_shared_for_metric_writes_and_reads(monkeypatch):
    """Durable serving must not silently write metric history to pod-local SQLite."""
    write_con = _FakePgConnection()
    read_con = _FakePgConnection(
        rows=[
            {
                "metric": "pool_wait_p95_ms",
                "service_id": "svc-a",
                "task": "",
                "ts": "2026-01-01T00:00:00Z",
                "value": 4.5,
            }
        ]
    )
    monkeypatch.setattr(metric_snapshots, "_pg_write_connection", lambda: write_con)
    monkeypatch.setattr(metric_snapshots, "_pg_connection", lambda: read_con)

    metric_snapshots.record_snapshot("pool_wait_p95_ms", 4.5, service_id="svc-a", ts="2026-01-01T00:00:00Z")
    out = metric_snapshots.get_batch(since=datetime(2025, 12, 31, tzinfo=UTC))

    assert out["pool_wait_p95_ms|svc-a"][0]["value"] == 4.5
    assert "ON CONFLICT" in write_con.calls[0][0]
    assert write_con.calls[0][1][2] == "svc-a"
    assert read_con.calls[0][1] == ("2025-12-31T00:00:00Z",)


# ── Merged from tests/core/test_metrics.py ──
def test_every_metric_type_has_a_definition_or_is_specific_status():
    """Every MetricType either appears in METRIC_DEFINITIONS or is
    ``specific_status`` (which is computed dynamically from status codes
    and never lives in the dict)."""
    for m in MetricType:
        assert m.value in METRIC_DEFINITIONS or m == MetricType.SPECIFIC_STATUS


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


# ── Merged from tests/core/test_request_context.py ──
pytestmark_for_tenancy = pytest.mark.security_regression


@pytest.mark.security_regression
def test_admin_request_with_service_id_passes():
    request = MagicMock()
    request.state = SimpleNamespace(analyst_session=None)
    assert _enforce_service_access(request, "svc-123") == "svc-123"


@pytest.mark.security_regression
def test_admin_request_without_service_id_raises_400():
    request = MagicMock()
    request.state = SimpleNamespace(analyst_session=None)
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, None)
    assert exc.value.status_code == 400
    assert exc.value.detail["error"] == "no_service"


@pytest.mark.security_regression
def test_scoped_analyst_with_authorized_service_passes():
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=["svc-1", "svc-2"]),
    )
    assert _enforce_service_access(request, "svc-1") == "svc-1"


@pytest.mark.security_regression
def test_scoped_analyst_with_unauthorized_service_raises_403():
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=["svc-1"]),
    )
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, "svc-other")
    assert exc.value.status_code == 403
    assert exc.value.detail["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_scoped_analyst_without_service_id_defaults_to_first_allowed():
    """Mirrors require_service_access semantics — an analyst calling a
    route that didn't pass an explicit service falls back to the first
    of their scoped services."""
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=["svc-7", "svc-8"]),
    )
    # set() order is not guaranteed; assert the result is one of allowed.
    out = _enforce_service_access(request, None)
    assert out in {"svc-7", "svc-8"}


@pytest.mark.security_regression
def test_scoped_analyst_with_empty_invite_raises_400():
    """An analyst session with no allowed services has nothing to default
    to; raise 400 rather than silently letting them through."""
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=[]),
    )
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, None)
    assert exc.value.status_code == 400


def test_request_context_carries_required_fields():
    """Constructor signature pins ADR-02's required attributes."""
    from backend.core.request_telemetry import RequestTelemetry

    ctx = RequestContext(
        service_id="svc-1",
        source={"name": "svc-1", "endpoint_url": "http://localhost"},
        _con_override=MagicMock(),
        telemetry=RequestTelemetry("GET", "/api/x"),
        analyst_session=None,
    )
    assert ctx.service_id == "svc-1"
    assert ctx.source["name"] == "svc-1"
    assert ctx.con is not None
    assert ctx.telemetry is not None
    assert ctx.analyst_session is None
    assert ctx.cached_temps == {}
    assert ctx.read_only is True


def test_cached_temps_are_per_instance():
    """Default factory yields a fresh dict per context — first repo's
    insertion can't leak to a later request's context."""
    from backend.core.request_telemetry import RequestTelemetry

    a = RequestContext(
        service_id="svc",
        source={"name": "svc"},
        _con_override=MagicMock(),
        telemetry=RequestTelemetry("GET", "/"),
    )
    b = RequestContext(
        service_id="svc",
        source={"name": "svc"},
        _con_override=MagicMock(),
        telemetry=RequestTelemetry("GET", "/"),
    )
    a.cached_temps["window:1h"] = "tmp_1234"
    assert "window:1h" not in b.cached_temps


def test_resolve_source_returns_source_dict_for_known_service():
    fake_source = {"name": "svc-x", "endpoint_url": "http://localhost"}
    with patch("backend.core.duckdb.get_source_for_service", return_value=fake_source):
        out = rc._resolve_source("svc-x")
    assert out is fake_source


def test_resolve_source_raises_400_for_unknown_service():
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        with pytest.raises(HTTPException) as exc:
            rc._resolve_source("svc-missing")
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


def _make_app_with_ctx_route() -> FastAPI:
    """Mini app that consumes RequestContext via a dependency. Used to
    pin the end-to-end shape — the route can't be reached without
    construction running tenancy enforcement first."""
    from fastapi import Depends
    from fastapi import Request as FRequest

    app = FastAPI()

    fake_source = {"name": "svc-1", "endpoint_url": "http://localhost"}

    @app.middleware("http")
    async def install_session(request: FRequest, call_next):
        # Test-only convenience header: x-test-session-services=svc-1,svc-2
        # → analyst_session with those service_ids
        sid_header = request.headers.get("x-test-session-services")
        if sid_header is not None:
            request.state.analyst_session = SimpleNamespace(
                service_ids=[s.strip() for s in sid_header.split(",") if s.strip()],
            )
        else:
            request.state.analyst_session = None
        return await call_next(request)

    @app.get("/api/test")
    def route(ctx: RequestContext = Depends(rc.build_request_context)):
        return {
            "service_id": ctx.service_id,
            "read_only": ctx.read_only,
            "is_admin": ctx.analyst_session is None,
        }

    # Patch service resolution + connection bridges so we don't need a
    # real DuckDB pool stood up for the TestClient.
    app.state._patches = []
    app.state._fake_source = fake_source
    return app


@pytest.mark.security_regression
def test_route_admin_request_with_explicit_service_succeeds(monkeypatch):
    app = _make_app_with_ctx_route()

    with (
        patch("backend.core.request_context._resolve_source", return_value=app.state._fake_source),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        client = TestClient(app)
        r = client.get("/api/test", params={"service": "svc-1"})

    assert r.status_code == 200
    body = r.json()
    assert body["service_id"] == "svc-1"
    assert body["read_only"] is True
    assert body["is_admin"] is True


@pytest.mark.security_regression
def test_route_analyst_request_with_unauthorized_service_403s(monkeypatch):
    """The structural Phase 2 invariant: there's no way to reach the
    route body for a service the analyst doesn't own."""
    app = _make_app_with_ctx_route()

    with (
        patch("backend.core.request_context._resolve_source", return_value=app.state._fake_source),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        client = TestClient(app)
        r = client.get(
            "/api/test",
            params={"service": "svc-OTHER"},
            headers={"x-test-session-services": "svc-1"},
        )

    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_route_read_only_cannot_be_overridden_by_query_param(monkeypatch):
    """The whole reason `read_only` is a constructor arg and NOT a dep
    param: an attacker passing ?read_only=false must NOT flip the
    in-flight connection mode. Verified by sending the bait and
    confirming the route still reports read_only=True."""
    app = _make_app_with_ctx_route()

    with (
        patch("backend.core.request_context._resolve_source", return_value=app.state._fake_source),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        client = TestClient(app)
        r = client.get(
            "/api/test",
            params={"service": "svc-1", "read_only": "false"},
        )

    assert r.status_code == 200
    assert r.json()["read_only"] is True


def _install_inspectable_pool(monkeypatch, service_key: str):
    """Register a real, inspectable ``_Pool`` for ``service_key`` seeded with
    one idle mock connection, with the pool force-enabled and the iceberg
    view-rebind probe stubbed so a checkout takes the fast (reused-conn) path
    without touching S3/Iceberg. Returns ``(pool, mock_con)``."""
    from backend.core import duckdb_pool

    pool = duckdb_pool._Pool(service_key=service_key, max_size=2)
    mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)
    # release()'s temp-table sweep calls con.execute(...).fetchall(); return
    # no rows so the sweep is a no-op against the mock.
    mock_con.execute.return_value.fetchall.return_value = []
    pool._idle.put_nowait(mock_con)
    pool._in_use = 1

    monkeypatch.setattr(duckdb_pool, "_pool_enabled", lambda: True)
    monkeypatch.setitem(duckdb_pool._pools, service_key, pool)
    # Skip the iceberg-view rebind probe on the reused-conn checkout path.
    monkeypatch.setattr(duckdb_pool._Pool, "_prepare_checkout", lambda self, con, src: con)
    return pool, mock_con


def _drive_request_context_to_yield(service_key: str):
    """Call ``build_request_context`` directly and advance the generator to
    its ``yield ctx`` (the point a real request hands control to the route).
    Returns the live generator so the test can close()/throw() into it."""
    request = MagicMock()
    request.state = SimpleNamespace(analyst_session=None)
    request.method = "GET"
    request.url.path = "/api/test"
    gen = rc.build_request_context(request, service_id=service_key)
    ctx = next(gen)  # runs setup + yields ctx
    return gen, ctx


def test_client_cancel_returns_connection_to_pool(monkeypatch):
    """POOL-3: a client cancel / request timeout unwinds
    ``build_request_context``'s generator via ``GeneratorExit`` at
    ``yield ctx``. Because ``GeneratorExit`` is a ``BaseException`` (not an
    ``Exception``), ``checkout_connection``'s ``except Exception`` is
    bypassed, ``errored`` stays False, and the connection is RETURNED to the
    idle pool — not discarded. Pins the ``with``/``try``/``finally`` nesting
    so a future refactor can't silently turn a cancel into a leaked slot or
    a needless ~150ms rebuild."""
    pool, mock_con = _install_inspectable_pool(monkeypatch, "svc-cancel")

    with patch(
        "backend.core.request_context._resolve_source",
        return_value={"name": "svc-cancel", "endpoint_url": "http://localhost"},
    ):
        gen, ctx = _drive_request_context_to_yield("svc-cancel")
        _ = ctx.con  # Trigger lazy load
        # Connection is checked out: idle drained, slot still owned.
        assert pool._in_use == 1
        assert pool._idle.qsize() == 0
        # Simulate the client cancel — GeneratorExit raised at `yield ctx`.
        gen.close()

    assert pool._in_use == 1, "slot still accounted (not leaked)"
    assert pool._idle.qsize() == 1, "clean cancel returns the conn to idle"
    assert pool._discarded_total == 0, "clean cancel must not discard a healthy conn"
    mock_con.close.assert_not_called()


def test_real_exception_discards_connection(monkeypatch):
    """Contrast to the cancel path: a genuine error thrown into the generator
    IS an ``Exception``, so ``checkout_connection`` marks the conn errored and
    the pool discards it — a poisoned connection never gets reused. Anchors
    the cancel-path assertion above by showing the discard branch still
    fires for real failures."""
    pool, mock_con = _install_inspectable_pool(monkeypatch, "svc-err")

    with patch(
        "backend.core.request_context._resolve_source",
        return_value={"name": "svc-err", "endpoint_url": "http://localhost"},
    ):
        gen, ctx = _drive_request_context_to_yield("svc-err")
        _ = ctx.con  # Trigger lazy load
        assert pool._in_use == 1
        with pytest.raises(RuntimeError, match="boom"):
            gen.throw(RuntimeError("boom"))

    assert pool._discarded_total == 1, "a real error discards the conn"
    assert pool._in_use == 0, "discard frees the slot"
    assert pool._idle.qsize() == 0
    mock_con.close.assert_called_once()


def test_prewarm_con_acquires_connection_off_the_event_loop():
    import asyncio
    import threading

    seen: dict[str, int] = {}

    class _Holder:
        con = None

        def __enter__(self):
            seen["thread"] = threading.get_ident()
            self.con = object()
            return self.con

    ctx = RequestContext(service_id="svc", source={}, telemetry=MagicMock(), _holder=_Holder())

    async def _run():
        loop_thread = threading.get_ident()
        await ctx.prewarm_con()
        return loop_thread

    loop_thread = asyncio.run(_run())

    assert seen["thread"] != loop_thread
    assert ctx.con is ctx._holder.con


def test_prewarm_con_cancellation_still_completes_checkout():
    import asyncio
    import threading

    release = threading.Event()
    entered = threading.Event()

    class _Holder:
        con = None

        def __enter__(self):
            entered.set()
            release.wait(5)
            self.con = object()
            return self.con

    ctx = RequestContext(service_id="svc", source={}, telemetry=MagicMock(), _holder=_Holder())

    async def _run():
        task = asyncio.create_task(ctx.prewarm_con())
        while not entered.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        loop = asyncio.get_running_loop()
        loop.call_later(0.1, release.set)
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run())

    assert ctx._holder.con is not None


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _ctx(time_bounds: TimeBounds | None, analyst: object | None) -> RequestContext:
    return make_test_request_context(
        service_id="svc",
        source={"name": "svc", "service_id": "svc"},
        session=analyst,
        time_bounds=time_bounds,
    )


def _req(**kw):
    base = {"range_token": None, "anchor": None, "start_time": None, "end_time": None}
    base.update(kw)
    return SimpleNamespace(**base)


def test_keyed_token_ignores_frontend_bounds(monkeypatch):
    monkeypatch.setattr(svcconfig, "get_status", lambda name: {})
    anchor = "2026-03-10T12:00:00Z"
    expected = resolve_window("24h", anchor)
    got = _ctx(None, None).resolve_window(
        _req(range_token="24h", anchor=anchor, start_time="2000-01-01T00:00:00Z", end_time="2000-01-02T00:00:00Z")
    )
    assert tuple(_dt(x) for x in got) == tuple(_dt(x) for x in expected)


def test_keyed_token_is_clamped_after_resolve_to_invite_window(monkeypatch):
    monkeypatch.setattr(svcconfig, "get_status", lambda name: {})
    anchor_dt = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    invite_start = anchor_dt - timedelta(hours=2)
    ctx = _ctx(TimeBounds(start=invite_start, end=anchor_dt), SimpleNamespace(session_id="s"))
    start, end = ctx.resolve_window(_req(range_token="30d", anchor="2026-03-10T12:00:00Z"))
    assert _dt(start) >= invite_start
    assert _dt(end) <= anchor_dt


def test_unknown_token_falls_back_to_clamped_absolute_bounds():
    ctx = _ctx(None, None)
    assert ctx.resolve_window(_req(range_token="bogus", start_time=None, end_time=None)) == (None, None)
    start, end = ctx.resolve_window(
        _req(range_token=None, start_time="2026-03-10T00:00:00Z", end_time="2026-03-10T06:00:00Z")
    )
    assert datetime.fromisoformat(start).hour == 0 and datetime.fromisoformat(end).hour == 6


# ── Merged from tests/core/test_request_metrics.py ──
@pytest.fixture
def durable_metrics(monkeypatch, tmp_path):
    sid = f"metrics-{uuid.uuid4().hex}"
    source = {
        "name": sid,
        "service_id": sid,
        "deployment_mode": "high_scale",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache"),
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
    }
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "configs")
    monkeypatch.setattr(config, "SERVICES_DATA_DIR", tmp_path / "services")
    config.SERVICES_DATA_DIR.mkdir()
    # Use the isolated worker Postgres DuckLake catalog while selecting
    # the production durable-only serving branch.
    monkeypatch.setattr(config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setattr(config, "config_to_source", lambda cfg: source)
    config.save_config(
        sid,
        {
            "service_id": sid,
            "status": {
                "rum": {"latest_log_at": "2026-09-09T00:00:00Z", "total_rows": 3},
                "request": {"latest_log_at": "2026-09-08T00:00:00Z", "total_rows": 99},
                "latest_log_at": "2026-09-08T00:00:00Z",
                "local_rows": 102,
            },
        },
    )
    cron = {"log_discovery": {"started_at": "2026-09-08T18:00:00Z"}}
    monkeypatch.setattr("backend.core.metadata.cron_log.latest_cron_per_task", lambda sid: cron)
    table = ducklake_table_name(source)

    def replace_rows(timestamps):
        con = duckdb.connect(":memory:")
        try:
            assert _ducklake_attach(con, source, read_only=False)
            con.execute(f'CREATE TABLE IF NOT EXISTS lake."{table}" (timestamp TIMESTAMPTZ)')
            con.execute(f'DELETE FROM lake."{table}"')
            for ts in timestamps:
                con.execute(f'INSERT INTO lake."{table}" VALUES (?)', [ts])
            info = con.execute(
                "SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = ?", [table]
            ).fetchone()
            assert info[0] in (0, 1, 2), "file_count is either inlined (0) or parquet (>=1)"
        finally:
            con.close()

    return source, replace_rows, cron, tmp_path


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_reads_inlined_rows_ignoring_local_parquet_and_native_file(durable_metrics):
    source, replace_rows, _, path = durable_metrics
    first = datetime(2026, 9, 7, 12, tzinfo=UTC)
    last = datetime(2026, 9, 7, 19, tzinfo=UTC)
    replace_rows([first, last])
    # A valid local parquet with a later event MUST NOT enter durable metrics.
    (path / "cache" / "buffer").mkdir(parents=True)
    con = duckdb.connect(":memory:")
    con.execute(
        "COPY (SELECT TIMESTAMPTZ '2026-09-10 00:00:00+00' AS timestamp) TO ? (FORMAT PARQUET)",
        [str(path / "cache" / "buffer" / "batch_local.parquet")],
    )
    con.close()
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["request"] == {
        "latest_log_at": last.isoformat(),
        "total_rows": 2,
        "last_sync_at": "2026-09-08T18:00:00Z",
    }
    assert observed["earliest_log_at"] == first.isoformat()
    assert observed["latest_log_at"] == last.isoformat()
    assert observed["local_rows"] == 5  # two REQUEST + three existing RUM
    assert not (path / "must-not-open.duckdb").exists()
    assert config.get_status(source["name"])["request"] == observed["request"]


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_uses_durable_table_when_serving_view_is_missing(durable_metrics, monkeypatch):
    source, replace_rows, _, _ = durable_metrics
    latest = datetime(2026, 9, 7, 19, tzinfo=UTC)
    replace_rows([latest])

    from backend.core.duckdb import open_serving_connection as real_open

    def open_without_serving_view(*args, **kwargs):
        con = real_open(*args, **kwargs)
        con.execute(f'DROP VIEW IF EXISTS "{ducklake_table_name(source)}"')
        return con

    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", open_without_serving_view)

    observed = request_metrics.refresh_durable_request_metrics(source)

    assert observed["request"]["total_rows"] == 1
    assert observed["request"]["latest_log_at"] == latest.isoformat()


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_reads_durable_rum_extents(durable_metrics):
    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, 12, tzinfo=UTC)])
    con = duckdb.connect(":memory:")
    try:
        assert _ducklake_attach(con, source, read_only=False)
        vitals_table = ducklake_table_name(source, "client_vitals")
        errors_table = ducklake_table_name(source, "client_errors")
        for table in (vitals_table, errors_table):
            con.execute(
                f'CREATE TABLE IF NOT EXISTS lake."{table}" (timestamp TIMESTAMPTZ, req_id VARCHAR, cid VARCHAR)'
            )
            con.execute(f'DELETE FROM lake."{table}"')
        con.execute(
            f'INSERT INTO lake."{vitals_table}" VALUES '
            "(TIMESTAMPTZ '2026-09-08 01:00:00+00', '', 'vital-1'), "
            "(TIMESTAMPTZ '2026-09-08 02:00:00+00', '', 'vital-2')"
        )
        con.execute(f"INSERT INTO lake.\"{errors_table}\" VALUES (TIMESTAMPTZ '2026-09-08 03:00:00+00', '', 'error-1')")
    finally:
        con.close()

    observed = request_metrics.refresh_durable_request_metrics(source)

    assert observed["rum"] == {
        "latest_log_at": "2026-09-08T03:00:00+00:00",
        "total_rows": 3,
        "last_sync_at": None,
    }
    assert config.get_status(source["name"])["rum"] == observed["rum"]


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_refreshes_rum_sync_time_from_latest_cron_run(durable_metrics):
    source, replace_rows, cron, _ = durable_metrics
    status = config.get_status(source["name"])
    status["rum"]["last_sync_at"] = "2026-09-07T00:00:00Z"
    config.update_status(source["name"], status)
    cron["rum_sync"] = {"started_at": "2026-09-12T15:00:00Z"}
    replace_rows([datetime(2026, 9, 7, 12, tzinfo=UTC)])

    observed = request_metrics.refresh_durable_request_metrics(source)

    assert observed["rum"]["last_sync_at"] == "2026-09-12T15:00:00Z"


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_accepts_backward_extent_reset_and_idle_cron(durable_metrics):
    source, replace_rows, cron, _ = durable_metrics
    latest = datetime(2026, 9, 7, tzinfo=UTC)
    older = datetime(2026, 9, 2, tzinfo=UTC)
    replace_rows([latest])
    request_metrics.refresh_durable_request_metrics(source)
    replace_rows([older])
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["latest_log_at"] == older.isoformat()
    replace_rows([])
    cron["log_discovery"]["started_at"] = "2026-09-08T18:01:00Z"
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["request"] == {
        "latest_log_at": None,
        "total_rows": 0,
        "last_sync_at": "2026-09-08T18:01:00Z",
    }
    assert observed["earliest_log_at"] is observed["latest_log_at"] is None
    assert observed["local_rows"] == 3  # RUM was not cleared by a request reset


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_failure_preserves_last_success(durable_metrics, monkeypatch):
    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, tzinfo=UTC)])
    request_metrics.refresh_durable_request_metrics(source)
    previous = config.get_status(source["name"])
    con = MagicMock()
    con.execute.side_effect = RuntimeError("catalog unavailable")
    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", lambda *a, **kw: con)
    with pytest.raises(RuntimeError, match="catalog unavailable"):
        request_metrics.refresh_durable_request_metrics(source)
    assert config.get_status(source["name"]) == previous
    con.close.assert_called_once()


@pytest.mark.usefixtures("_no_real_kill")
def test_legacy_heavy_refresh_cannot_overwrite_durable_request_metrics(durable_metrics, monkeypatch):
    from backend.core._duckdb_status import refresh_config_status

    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, tzinfo=UTC)])
    monkeypatch.setattr(
        "backend.core.duckdb.get_sync_status",
        MagicMock(side_effect=AssertionError("local/stale stats path called")),
    )
    monkeypatch.setattr("backend.core.iceberg.get_table_info", lambda src: {})
    monkeypatch.setattr("backend.repositories.usage.get_edge_ratio", lambda *a: (None, None))
    refresh_config_status(source["name"], include_top_values=False)
    assert config.get_status(source["name"])["request"]["total_rows"] == 1
    replace_rows([])
    refresh_config_status(source["name"], include_top_values=False)
    status = config.get_status(source["name"])
    assert status["request"]["total_rows"] == 0
    assert status["request"]["latest_log_at"] is status["latest_log_at"] is None


@pytest.mark.usefixtures("_no_real_kill")
def test_shared_refresh_coalesces_other_status_refreshes(durable_metrics, monkeypatch):
    source, _, _, _ = durable_metrics
    open_con = MagicMock(side_effect=AssertionError("overlapping scan"))
    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", open_con)
    with request_metrics._refresh_lock:
        assert request_metrics.refresh_durable_request_metrics(source) is None
    open_con.assert_not_called()


@pytest.mark.usefixtures("_no_real_kill")
def test_query_deadline_interrupts_without_persisting_zero(durable_metrics, monkeypatch):
    source, _, _, _ = durable_metrics
    previous = config.get_status(source["name"])
    raw = duckdb.connect(":memory:")
    interrupted = []

    class SlowConnection:
        def execute(self, sql, *args, **kwargs):
            if sql.startswith("SELECT count(*)"):
                sql = "SELECT sum(sin(i)) FROM range(1000000000) t(i)"
            return raw.execute(sql, *args, **kwargs)

        def interrupt(self):
            interrupted.append(True)
            raw.interrupt()

        def close(self):
            raw.close()

    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", lambda *a, **kw: SlowConnection())
    monkeypatch.setattr(request_metrics, "QUERY_TIMEOUT_S", 0.05)
    with pytest.raises(duckdb.InterruptException):
        request_metrics.refresh_durable_request_metrics(source)
    assert interrupted
    assert config.get_status(source["name"]) == previous


@pytest.mark.usefixtures("_no_real_kill")
def test_real_observation_is_persisted_before_sse_publication(durable_metrics, monkeypatch):
    from backend import request_metrics_observer as module

    source, replace_rows, _, _ = durable_metrics
    latest = datetime(2026, 9, 7, tzinfo=UTC)
    replace_rows([latest])
    published = []

    def publish(service_id, payload):
        persisted = config.get_status(service_id)
        assert persisted["request"] == payload["request"]
        assert persisted["latest_log_at"] == payload["latest_log_at"]
        published.append(payload["request"])

    monkeypatch.setattr(module, "compute_sync_status_cached", config.get_status)
    monkeypatch.setattr(module.publisher, "publish", publish)
    observer = module.RequestMetricsObserver()
    observer.reconcile()
    observer.reconcile()
    assert len(published) == 1
    assert published[0]["latest_log_at"] == latest.isoformat()
    replace_rows([])
    observer.reconcile()
    assert len(published) == 2
    assert published[-1]["total_rows"] == 0
    assert published[-1]["latest_log_at"] is None


class _FakeGauge:
    def __init__(self, callbacks):
        self.callbacks = callbacks


class _FakeMeter:
    def __init__(self):
        self.gauges = {}

    def create_observable_gauge(self, *, name, callbacks, description, unit):
        gauge = _FakeGauge(callbacks)
        self.gauges[name] = (gauge, description, unit)
        return gauge


@pytest.mark.usefixtures("_no_real_kill")
def test_record_exposes_latest_labeled_observation(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record("celery_queue_depth", 7, queue="q.ingest")

    gauge, description, unit = meter.gauges["fla_celery_queue_depth"]
    observations = gauge.callbacks[0](None)
    assert [(item.value, item.attributes) for item in observations] == [(7.0, {"queue": "q.ingest"})]
    assert description == "Celery broker queue depth"
    assert unit == "{messages}"


@pytest.mark.usefixtures("_no_real_kill")
def test_record_keeps_distinct_label_sets(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record("ingest_ledger_rows", 3, status="discovered")
    operational_metrics.record("ingest_ledger_rows", 2, status="claimed")

    gauge, _, _ = meter.gauges["fla_ingest_ledger_rows"]
    observations = gauge.callbacks[0](None)
    assert {(item.value, item.attributes["status"]) for item in observations} == {
        (3.0, "discovered"),
        (2.0, "claimed"),
    }


@pytest.mark.usefixtures("_no_real_kill")
def test_high_scale_metrics_are_exported_with_domain_labels(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)

    operational_metrics.record(
        "high_scale_publication_lag_seconds",
        12.5,
        service_id="svc",
        domain="request",
    )

    gauge, description, unit = meter.gauges["fla_high_scale_publication_lag_seconds"]
    observation = gauge.callbacks[0](None)[0]
    assert observation.value == 12.5
    assert observation.attributes == {"domain": "request", "service_id": "svc"}
    assert description == "Age of the oldest pending high-scale publication"
    assert unit == "s"


@pytest.mark.usefixtures("_no_real_kill")
def test_telemetry_schema_and_helpers(monkeypatch, tmp_path):
    # Setup temporary directory for metadata DB
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))

    # Initialize connection which runs schemas
    con = usage_log_db.get_con("test_service")

    # Verify tables exist
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
        ).fetchall()
    ]
    assert "telemetry_queries" in tables
    assert "telemetry_sections" in tables

    # Test logging queries
    usage_log.log_telemetry_query(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        engine="duckdb",
        sql="SELECT 1",
        duration_ms=45.2,
    )

    # Test logging sections
    usage_log.log_telemetry_section(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        section_name="dashboard.aggregates",
        duration_ms=12.5,
    )

    # Test retrieval
    telemetry = usage_log.get_page_telemetry("test_service", "page-123")
    assert len(telemetry["queries"]) == 1
    assert telemetry["queries"][0]["sql_query"] == "SELECT 1"
    assert telemetry["queries"][0]["engine"] == "duckdb"
    assert telemetry["queries"][0]["duration_ms"] == 45.2

    assert len(telemetry["sections"]) == 1
    assert telemetry["sections"][0]["section_name"] == "dashboard.aggregates"
    assert telemetry["sections"][0]["duration_ms"] == 12.5


@pytest.mark.usefixtures("_no_real_kill")
def test_page_load_id_propagation_middleware():
    client = TestClient(app)
    response = client.get("/api/health", headers={"X-Page-Load-ID": "test-page-999"})
    assert response.status_code == 200
    assert get_page_load_id() is None


@pytest.fixture
def _no_real_kill(monkeypatch):
    """Hard safety net: never let a real signal escape this module under test."""
    calls = []
    monkeypatch.setattr(memory_guard.os, "kill", lambda pid, sig: calls.append((pid, sig)))
    return calls


def _set_rss(monkeypatch, value):
    """Patch the lazily-imported current_rss_bytes to return ``value`` bytes."""
    monkeypatch.setattr("backend.core.duckdb.current_rss_bytes", lambda: value)


@pytest.mark.usefixtures("_no_real_kill")
def test_disabled_by_default(monkeypatch, _no_real_kill):
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    _set_rss(monkeypatch, 50_000 * 1024 * 1024)  # huge, but guard is off
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


@pytest.mark.usefixtures("_no_real_kill")
def test_below_threshold_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 5000 * 1024 * 1024)  # 5GB < 9GB
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


@pytest.mark.usefixtures("_no_real_kill")
def test_at_or_above_threshold_sends_sigterm(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 9500 * 1024 * 1024)  # 9.5GB >= 9GB
    assert memory_guard.maybe_graceful_restart() is True
    assert len(_no_real_kill) == 1
    pid, sig = _no_real_kill[0]
    assert sig == signal.SIGTERM
    assert pid > 0


@pytest.mark.usefixtures("_no_real_kill")
def test_unreadable_rss_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, None)  # /proc unreadable (e.g. off-Linux)
    assert memory_guard.maybe_graceful_restart() is False
    assert _no_real_kill == []


@pytest.mark.usefixtures("_no_real_kill")
def test_threshold_parse_bytes(monkeypatch):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 9000 * 1024 * 1024
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "garbage")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0


@pytest.mark.usefixtures("_no_real_kill")
@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", "On"])
def test_collection_enabled_truthy_values(monkeypatch, value):
    monkeypatch.setenv("WEB_VITALS_COLLECT", value)
    assert web_vitals_store.collection_enabled() is True


@pytest.mark.usefixtures("_no_real_kill")
@pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
def test_collection_enabled_falsey_values(monkeypatch, value):
    monkeypatch.setenv("WEB_VITALS_COLLECT", value)
    assert web_vitals_store.collection_enabled() is False


@pytest.mark.usefixtures("_no_real_kill")
def test_collection_disabled_when_unset(monkeypatch):
    monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    assert web_vitals_store.collection_enabled() is False


@pytest.mark.usefixtures("_no_real_kill")
def test_append_sample_writes_one_json_line_each(monkeypatch, tmp_path):
    sink = tmp_path / "nested" / "web_vitals.jsonl"  # parent dir is created lazily
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)

    web_vitals_store.append_sample({"name": "LCP", "value": 1200.0, "rating": "good"})
    web_vitals_store.append_sample({"name": "CLS", "value": 0.05, "rating": "good"})

    lines = sink.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["name"] == "LCP"
    assert json.loads(lines[1])["name"] == "CLS"


@pytest.mark.usefixtures("_no_real_kill")
def test_append_sample_never_raises_on_io_error(monkeypatch, tmp_path):
    # Point LOG_PATH at a path whose parent is a file, so mkdir/open fails;
    # the helper must swallow the error rather than break the request.
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", blocker / "child" / "wv.jsonl")
    web_vitals_store.append_sample({"name": "LCP", "value": 1.0, "rating": "good"})  # no exception


@pytest.mark.usefixtures("_no_real_kill")
def test_max_bytes_defaults_to_200mb(monkeypatch):
    monkeypatch.delenv("WEB_VITALS_MAX_MB", raising=False)
    assert web_vitals_store._max_bytes() == 200 * 1024 * 1024


@pytest.mark.usefixtures("_no_real_kill")
def test_max_bytes_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "not-a-number")
    assert web_vitals_store._max_bytes() == 200 * 1024 * 1024


@pytest.mark.usefixtures("_no_real_kill")
def test_max_bytes_zero_disables_cap(monkeypatch):
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0")
    assert web_vitals_store._max_bytes() == 0


@pytest.mark.usefixtures("_no_real_kill")
def test_rotation_creates_single_backup_at_cap(monkeypatch, tmp_path):
    """Once the active file reaches the cap it's rotated to a single .1
    backup and a fresh file continues — recent data is retained, disk is
    bounded at ~2x the cap."""
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", tmp_path / "wv.jsonl")
    # ~52-byte cap: smaller than one sample line, so each write past the
    # first rotates. Exercises the rotation path deterministically.
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0.00005")
    for i in range(4):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})

    active = tmp_path / "wv.jsonl"
    backup = web_vitals_store.rotated_path()
    assert active.exists()
    assert backup.exists()  # rotation happened
    # Only ONE backup is kept, and the active file holds the most recent
    # write(s) — not all four.
    assert len(active.read_text(encoding="utf-8").strip().splitlines()) < 4
    assert not (tmp_path / "wv.jsonl.2").exists()


@pytest.mark.usefixtures("_no_real_kill")
def test_cap_zero_disables_rotation(monkeypatch, tmp_path):
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", tmp_path / "wv.jsonl")
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0")
    for i in range(5):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})
    assert not web_vitals_store.rotated_path().exists()
    assert len((tmp_path / "wv.jsonl").read_text(encoding="utf-8").strip().splitlines()) == 5


# ── Merged from tests/core/test_request_telemetry.py ──
@pytest.fixture
def _reset_module():
    """Reset module-level lazy state so SDK setup is re-attempted per test."""
    request_telemetry._initialised = False
    request_telemetry._thread_wait_histogram = None
    yield
    request_telemetry._initialised = False
    request_telemetry._thread_wait_histogram = None


@pytest.mark.usefixtures("_reset_module")
def test_otel_disabled_under_pytest_by_default(monkeypatch):
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "true")
    assert request_telemetry._otel_enabled() is False


@pytest.mark.usefixtures("_reset_module")
def test_force_flush_existing_providers_never_initializes_or_raises(monkeypatch):
    meter, tracer = MagicMock(), MagicMock()
    monkeypatch.setattr(request_telemetry.metrics, "get_meter_provider", lambda: meter)
    monkeypatch.setattr(request_telemetry.trace, "get_tracer_provider", lambda: tracer)
    request_telemetry.force_flush()
    assert not request_telemetry._initialised
    meter.force_flush.assert_called_once_with(timeout_millis=5000)
    tracer.force_flush.assert_called_once_with(timeout_millis=5000)
    meter.force_flush.side_effect = RuntimeError("export unavailable")
    request_telemetry.force_flush()
    assert tracer.force_flush.call_count == 2


@pytest.mark.usefixtures("_reset_module")
def test_otel_enabled_requires_exporter_to_be_set(monkeypatch):
    """The default OTEL_EXPORTER ('none') keeps the SDK uninstalled even
    when OTEL_ENABLED=1 — the old code spammed prod stdout with the
    ConsoleSpanExporter because exporter installation wasn't gated."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.delenv("OTEL_EXPORTER", raising=False)
    assert request_telemetry._otel_enabled() is False


@pytest.mark.usefixtures("_reset_module")
def test_otel_enabled_when_exporter_is_console(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    assert request_telemetry._otel_enabled() is True


@pytest.mark.usefixtures("_reset_module")
def test_otel_enabled_when_exporter_is_otlp(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "otlp")
    assert request_telemetry._otel_enabled() is True


@pytest.mark.usefixtures("_reset_module")
def test_asgi_instrumentation_enabled_mirrors_otel_enabled(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "otlp")
    assert request_telemetry.asgi_instrumentation_enabled() is True

    monkeypatch.setenv("OTEL_EXPORTER", "none")
    assert request_telemetry.asgi_instrumentation_enabled() is False


@pytest.mark.usefixtures("_reset_module")
def test_ensure_initialized_runs_setup_sdk_once(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "console")

    request_telemetry.ensure_initialized()
    assert request_telemetry._initialised is True


@pytest.mark.usefixtures("_reset_module")
def test_sdk_resource_separates_short_lived_cli_and_backend_counters(monkeypatch):
    monkeypatch.setattr(request_telemetry, "_otel_enabled", lambda: True)
    monkeypatch.setattr(request_telemetry, "_otel_exporter", lambda: "none")
    with (
        patch.object(request_telemetry.trace, "set_tracer_provider"),
        patch.object(request_telemetry.metrics, "set_meter_provider") as setter,
        patch.object(request_telemetry, "TracerProvider"),
        patch.object(request_telemetry, "MeterProvider") as provider,
    ):
        request_telemetry.ensure_initialized()
        first = provider.call_args.kwargs["resource"].attributes["service.instance.id"]
        request_telemetry.ensure_initialized()
        setter.assert_called_once()
        request_telemetry._initialised = False
        request_telemetry.ensure_initialized()
        second = provider.call_args.kwargs["resource"].attributes["service.instance.id"]
        assert first and second and first != second


@pytest.mark.usefixtures("_reset_module")
def test_sdk_resource_includes_elevation_identity(monkeypatch):
    monkeypatch.setattr(request_telemetry, "_otel_enabled", lambda: True)
    monkeypatch.setattr(request_telemetry, "_otel_exporter", lambda: "none")
    monkeypatch.setenv("ELEVATION_CLUSTER_NAME", "dev-usc1")
    monkeypatch.setenv("POD_NAMESPACE", "test-namespace")
    with (
        patch.object(request_telemetry.trace, "set_tracer_provider"),
        patch.object(request_telemetry.metrics, "set_meter_provider"),
        patch.object(request_telemetry, "TracerProvider"),
        patch.object(request_telemetry, "MeterProvider") as provider,
    ):
        request_telemetry.ensure_initialized()
        attributes = provider.call_args.kwargs["resource"].attributes
        assert attributes["site"] == "elevation-dev-usc1"
        assert attributes["namespace"] == "test-namespace"


@pytest.mark.usefixtures("_reset_module")
def test_setup_sdk_otlp_installs_otlp_exporters(monkeypatch):
    """OTEL_EXPORTER=otlp must construct the OTLP span + metric exporters
    (endpoints resolve from the standard OTEL_EXPORTER_OTLP_* env vars).
    The global provider setters are stubbed so no SDK state leaks between
    tests, and the exporter classes are mocked so nothing dials out."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://metrics.example.test/v1/metrics")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://traces.example.test/v1/traces")

    with (
        patch("opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter") as span_exporter,
        patch("opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter") as metric_exporter,
        patch("backend.core.request_telemetry.PeriodicExportingMetricReader") as metric_reader,
        patch.object(request_telemetry.trace, "set_tracer_provider") as set_tracer,
        patch.object(request_telemetry.metrics, "set_meter_provider") as set_meter,
    ):
        request_telemetry._setup_sdk()

    span_exporter.assert_called_once_with(endpoint="https://traces.example.test/v1/traces")
    metric_exporter.assert_called_once_with(endpoint="https://metrics.example.test/v1/metrics")
    metric_reader.assert_called_once()
    set_tracer.assert_called_once()
    set_meter.assert_called_once()


@pytest.mark.parametrize(
    "missing_endpoint",
    ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"],
)
@pytest.mark.usefixtures("_reset_module")
def test_setup_sdk_otlp_requires_explicit_endpoints(monkeypatch, missing_endpoint):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://metrics.example.test/v1/metrics")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://traces.example.test/v1/traces")
    monkeypatch.delenv(missing_endpoint)

    with pytest.raises(ValueError, match=missing_endpoint):
        request_telemetry._setup_sdk()

    assert request_telemetry._initialised is False


@pytest.mark.usefixtures("_reset_module")
def test_otel_master_switch_off_overrides_exporter(monkeypatch):
    """OTEL_ENABLED=0 wins even if an exporter is configured."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "0")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    assert request_telemetry._otel_enabled() is False


@pytest.mark.usefixtures("_reset_module")
def test_get_tracer_returns_a_tracer():
    """No SDK installed in test mode → tracer returns NonRecordingSpan
    when spans are started. Public surface still works."""
    tracer = request_telemetry.get_tracer()
    assert tracer is not None


@pytest.mark.usefixtures("_reset_module")
def test_setup_sdk_is_idempotent():
    request_telemetry._setup_sdk()
    request_telemetry._setup_sdk()
    request_telemetry._setup_sdk()


@pytest.mark.usefixtures("_reset_module")
def test_request_lifecycle_in_test_mode():
    """In test mode, spans don't record; the public API still returns
    sensible shapes so callers don't have to special-case."""
    ctx = request_telemetry.RequestTelemetry("GET", "/api/dashboard/aggregates")
    ctx.start_request()
    ctx.start_request()  # idempotent
    ctx.end_request(status_code=200)
    ctx.end_request()  # idempotent


@pytest.mark.usefixtures("_reset_module")
def test_thread_wait_histogram_constructed_once():
    """The lazy property should construct the instrument on first call
    and return the same object thereafter."""
    h1 = request_telemetry.thread_wait_histogram()
    h2 = request_telemetry.thread_wait_histogram()
    assert h1 is h2


@pytest.mark.usefixtures("_reset_module")
def test_start_request_reuses_ambient_span_instead_of_opening_a_second_one():
    """When something upstream (FastAPIInstrumentor's ASGI middleware) has
    already opened a recording span before start_request runs, reuse it
    instead of opening a duplicate — otherwise a span-metrics processor
    would count every such request twice. Only the span this instance
    actually opened should ever get end()ed."""
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    ambient_tracer = provider.get_tracer("ambient")

    ambient_span = ambient_tracer.start_span("GET /api/ambient-example")
    with (
        trace.use_span(ambient_span, end_on_exit=False),
        patch("backend.core.request_telemetry.get_tracer") as mock_get_tracer,
    ):
        ctx = request_telemetry.RequestTelemetry("GET", "/api/ambient-example")
        ctx.start_request()
        ctx.end_request(status_code=200)

    mock_get_tracer.assert_not_called()
    assert exporter.get_finished_spans() == ()  # end_request must not have ended it

    ambient_span.end()
    (finished,) = exporter.get_finished_spans()
    assert finished.name == "GET /api/ambient-example"
    assert finished.attributes is not None
    assert finished.attributes.get("http.status_code") == 200
    assert finished.attributes.get("app.is_cached") is False


@pytest.mark.usefixtures("_reset_module")
def test_thread_wait_histogram_records_observation_via_meter_provider():
    """Drive ``thread_wait_histogram().record(...)`` against an in-memory
    MetricReader and assert the observation is collected with the
    expected attributes. Closes the audit gap: today the histogram is
    instantiated correctly (test_thread_wait_histogram_constructed_once)
    but no test asserts the recorded values flow through to the meter.
    A regression that broke the meter wiring would silently zero out
    ADR-03's app.thread_wait_ms percentiles without surfacing.
    """
    from opentelemetry import metrics as ot_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    # Build a private meter provider with an in-memory reader so we don't
    # touch the module-level global SDK state (wedge-prone under xdist).
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])

    # Reset the cached histogram + meter so the instrument is rebuilt
    # against our private provider; restore both at the end.
    saved_histogram = request_telemetry._thread_wait_histogram
    saved_meter_get = request_telemetry.get_meter

    try:
        request_telemetry._thread_wait_histogram = None
        request_telemetry.get_meter = lambda: provider.get_meter("test-meter")  # type: ignore[assignment]

        h = request_telemetry.thread_wait_histogram()
        # Two distinct observations with different attributes.
        h.record(12.5, {"service": "svc-a", "outcome": "reused"})
        h.record(45.0, {"service": "svc-a", "outcome": "created"})
        h.record(125.0, {"service": "svc-b", "outcome": "timeout"})

        data = reader.get_metrics_data()
        assert data is not None, "InMemoryMetricReader produced no metrics_data"

        # Collect every observation across the (single) resource + scope.
        histogram_metrics = []
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    histogram_metrics.append(m)

        # The instrument must appear and carry our three observations.
        names = [m.name for m in histogram_metrics]
        assert any("thread_wait" in n.lower() or "wait" in n.lower() for n in names), (
            f"thread_wait histogram missing from collected metrics; got names={names!r}"
        )

        # At least one of the data points must show our recorded count >= 3.
        total_observed = 0
        for m in histogram_metrics:
            for dp in getattr(m.data, "data_points", []):
                total_observed += dp.count
        assert total_observed >= 3, (
            f"expected ≥ 3 observations recorded across all data points; total_observed={total_observed}"
        )
    finally:
        request_telemetry._thread_wait_histogram = saved_histogram
        request_telemetry.get_meter = saved_meter_get  # type: ignore[assignment]
        ot_metrics  # noqa: B018 — touch import so linters don't drop it after refactor


# ── Merged from tests/core/test_query_instrumentation.py ──
@pytest.fixture
def duck() -> duckdb.DuckDBPyConnection:
    """A throwaway in-memory DuckDB connection per test."""
    con = duckdb.connect(":memory:")
    yield con
    try:
        con.close()
    except Exception:
        pass


def test_finish_is_idempotent_no_double_deregister():
    """Calling ``_finish`` twice must deregister exactly once. Without
    the ``self._done`` guard, a terminal method followed by ``__del__``
    (or two terminal methods in a row) would double-pop the registry
    and corrupt by-db-type counts."""
    fake_raw = MagicMock()
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result = _InstrumentedResult(fake_raw, qid=7, con_ref=None)
        result._finish(None, probe_memory=False)
        result._finish(None, probe_memory=False)
        # And a third call (the safety-net __del__ path) is also a no-op.
        result._finish(None, probe_memory=False)
    assert mock_dereg.call_count == 1
    assert mock_dereg.call_args.args == (7, None)


def test_arrow_reader_defers_deregister_until_exhausted(duck):
    """``.arrow()`` returns a streaming reader; the proxy must NOT
    deregister at the method-call boundary. Without _InstrumentedRecordReader
    the monitor would attribute ~0ms to long streams."""
    duck.execute("CREATE TABLE t AS SELECT i FROM range(50_000) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="defer_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        reader = con.execute("SELECT * FROM t").arrow()
        # Returning the reader does NOT deregister.
        assert mock_dereg.call_count == 0
        assert isinstance(reader, _InstrumentedRecordReader)
        # Iterate — deregister fires exactly once at the end.
        total = sum(b.num_rows for b in reader)
        assert total == 50_000
        assert mock_dereg.call_count == 1


def test_fetch_record_batch_defers_until_drained(duck):
    """``fetch_record_batch`` is in _READER_METHODS too; deregistration
    waits for the consumer to drain via read_next_batch()."""
    duck.execute("CREATE TABLE t AS SELECT i FROM range(1_000) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="frb_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        reader = con.execute("SELECT * FROM t").fetch_record_batch(100)
        assert mock_dereg.call_count == 0
        # Drain by iterating (RecordBatchReader supports iteration).
        rows = sum(b.num_rows for b in reader)
        assert rows == 1_000
        assert mock_dereg.call_count == 1


@pytest.mark.parametrize("method", ["fetchall", "fetchone", "fetchnumpy"])
def test_terminal_method_deregisters_immediately(duck, method):
    """fetchall / fetchone / fetchnumpy are in _TERMINAL_METHODS — they
    must deregister at the call boundary (not after some later GC)."""
    duck.execute("CREATE TABLE t AS SELECT i FROM range(10) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="term_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result = con.execute("SELECT i FROM t")
        getattr(result, method)()
        assert mock_dereg.call_count == 1
        # The single deregister call carried no error.
        assert mock_dereg.call_args.args[1] is None


def test_execute_exception_deregisters_with_error(duck):
    """When the underlying ``.execute`` raises, the proxy must still
    deregister AND pass the exception so the registry records
    ``outcome="error"`` rather than leaking the row as active forever."""
    con = InstrumentedDuckDBConnection(duck, service_id="err_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM table_that_does_not_exist_xyz")
        assert mock_dereg.call_count == 1
        # Second positional arg is the error instance.
        passed_err = mock_dereg.call_args.args[1]
        assert isinstance(passed_err, BaseException)


def test_finish_safe_when_underlying_con_gc_collected():
    """The proxy keeps only a weakref to the raw connection (so the
    pool can free it on error). When the raw con is GC'd before
    ``_finish`` runs, the probe must short-circuit on ``con is None``
    rather than crashing — instrumentation must never crash the caller."""

    class _WeakRefable:
        def cursor(self):  # never called — weakref dies before probe runs
            raise AssertionError("probe ran on a dead connection")

    raw = _WeakRefable()
    ref = _safe_weakref(raw)
    assert ref is not None and ref() is raw  # weakref worked

    fake_inner = MagicMock()
    result = _InstrumentedResult(fake_inner, qid=42, con_ref=ref)

    del raw
    gc.collect()
    assert ref() is None  # underlying con collected

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        # probe_memory=True forces the con_ref deref branch; must not raise.
        result._finish(None, probe_memory=True)
    assert mock_dereg.call_count == 1
    # peak_memory_mb kwarg defaults to None when the con is gone.
    assert mock_dereg.call_args.kwargs.get("peak_memory_mb") is None


def test_safe_weakref_falls_back_to_strong_ref_for_non_weakrefable():
    """Some objects (e.g. plain ints, sqlite3 connections in some
    builds) reject weakref. The helper must return a callable that
    yields the object rather than ``None`` — otherwise the probe path
    silently no-ops for whole classes of connections."""
    not_weakrefable = 12345  # int rejects weakref.ref
    with pytest.raises(TypeError):
        weakref.ref(not_weakrefable)
    ref = _safe_weakref(not_weakrefable)
    assert ref is not None
    assert ref() == 12345


def test_probe_returns_none_on_malformed_pragma_row():
    """If the PRAGMA row comes back with an unparseable byte value the
    probe must return ``None`` rather than propagate ValueError out of
    the SQL hot path (the finally-block on .execute calls this)."""
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value = fake_cursor
    # Row exists but the byte value is garbage that _parse_memory_mb rejects.
    fake_cursor.execute.return_value.fetchone.return_value = ("not-a-number",)
    assert _probe_duckdb_memory(fake_con) is None


def test_probe_returns_none_on_none_row():
    """fetchone() returning None (empty result set) must short-circuit
    to None, not raise IndexError."""
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value = fake_cursor
    fake_cursor.execute.return_value.fetchone.return_value = None
    assert _probe_duckdb_memory(fake_con) is None


def test_probe_returns_none_on_null_byte_value():
    """duckdb_memory() may report a NULL sum (empty connection state) —
    treat as 'no number to record' instead of crashing on float(None)."""
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value = fake_cursor
    fake_cursor.execute.return_value.fetchone.return_value = (None,)
    assert _probe_duckdb_memory(fake_con) is None


def test_probe_returns_none_when_cursor_raises():
    """Pre-1.0 DuckDB has no duckdb_memory() view — cursor.execute()
    raises. The probe must swallow and return None so older builds
    don't break instrumentation."""
    fake_con = MagicMock()
    fake_con.cursor.side_effect = RuntimeError("no such function: duckdb_memory")
    assert _probe_duckdb_memory(fake_con) is None


# ── Merged from tests/core/test_slow_queries_optimizations.py ──
def test_usage_log_purging_and_trigger_restricton(tmp_path, monkeypatch):
    """Verify raw log deletes do not decrement hourly summary, but reconciliation deletes do."""
    from datetime import UTC, datetime, timedelta

    from backend.utils.date_utils import iso_z

    sid = "test_purge_svc"
    usage_log.clear_usage_log(sid)

    old_ts = "2020-05-01T10:00:00Z"
    now_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    now_hour = now_dt.strftime("%Y-%m-%dT%H")
    now_ts_1 = iso_z(now_dt)
    now_ts_2 = iso_z(now_dt + timedelta(minutes=15))

    con = usage_log_db.get_con(sid)
    try:
        # 1. Insert raw logs and reconciliation rows with matching summary records
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (old_ts, sid, "A", "CDN", 100, "api.sync"),
        )
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (now_ts_2, sid, "A", "RECONCILE_A", 50, "fastly.reconciliation"),
        )
        con.execute(
            """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sid, "2020-05-01T10", "A", "CDN", 100, 0, old_ts),
        )
        con.execute(
            """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sid, now_hour, "A", "RECONCILE_A", 50, 0, now_ts_1),
        )
        con.commit()

        # Check summaries aggregated both (150 total)
        row = con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 150

        # 2. Delete raw log row (simulating retention purging of old rows)
        usage_log.purge_usage_log(sid, retention_days=30)

        # The summary MUST remain untouched (still 150)
        row = con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 150

        # 3. Reconcile fastly stats (simulating gap recompute where Fastly reports 0 -> gap 0, so old 50 removed)
        usage_log.reconcile_fastly_stats(sid, [{"hour_iso": now_ts_1, "class_a": 0, "class_b": 0}])

        # The summary MUST reflect the reconciliation adjustment (the old 50 reconciliation row is gone -> total 100)
        row = con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 100
    finally:
        usage_log_db.close_all_connections()


def test_clear_usage_log_wipes_both_tables(tmp_path, monkeypatch):
    """Verify that clear_usage_log explicitly truncates both usage_log and usage_log_hourly_summary."""
    sid = "test_clear_svc"
    con = usage_log_db.get_con(sid)
    try:
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("2026-05-01T10:00:00Z", sid, "A", "CDN", 100, "api.sync"),
        )
        con.execute(
            """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sid, "2026-05-01T10", "A", "CDN", 100, 0, "2026-05-01T10:00:00Z"),
        )
        con.commit()

        # Summaries aggregated
        row = con.execute("SELECT count FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 100
    finally:
        usage_log_db.close_all_connections()

    # Call clear_usage_log
    clear_usage_log(sid)

    # Connect again and verify both are empty
    con = usage_log_db.get_con(sid)
    try:
        assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
        assert (
            con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 0
        )
    finally:
        usage_log_db.close_all_connections()


def test_flush_releases_postgres_thread_connection_after_success(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)

    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-flush"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    con.executemany.assert_called_once()
    con.commit.assert_called_once_with()
    release.assert_called_once_with()


def test_flush_releases_connection_when_write_fails(monkeypatch):
    con = MagicMock()
    con.executemany.side_effect = RuntimeError("metadata unavailable")
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)

    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-flush-error"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()


def test_flush_releases_on_the_short_lived_worker_thread(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    released_on = []

    def release():
        released_on.append(threading.current_thread())

    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-thread"] = [{"query_id": "q1"}]

    worker = threading.Thread(target=slow_queries._flush_all)
    worker.start()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert released_on == [worker]


def test_flush_without_connection_keeps_best_effort_behavior(monkeypatch):
    monkeypatch.setattr(
        slow_queries,
        "get_con",
        MagicMock(side_effect=RuntimeError("metadata unavailable")),
    )
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-no-connection"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()
