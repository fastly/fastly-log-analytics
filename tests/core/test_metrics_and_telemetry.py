"""Unit tests for metric expressions, metric snapshots, metrics SQL, request context, request metrics, telemetry, query instrumentation, and slow queries optimizations."""

from __future__ import annotations

import asyncio
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
from backend.core.metric_expressions import METRIC_DEFINITIONS, get_metric_sql
from backend.models._base import MetricType
from fastapi import Depends, FastAPI, HTTPException
from fastapi import Request as FRequest
from fastapi.testclient import TestClient
from opentelemetry import metrics as ot_metrics
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from tests._helpers.config_fakes import fetch_metric_snapshot_history as _get_history

from backend import config
from backend import config as svcconfig
from backend import request_metrics_observer as observer_mod
from backend.core import (
    duckdb_pool,
    metric_expressions,
    metric_snapshots,
    request_metrics,
    request_telemetry,
)
from backend.core import (
    duckdb_recycle as memory_guard,
)
from backend.core import (
    metric_snapshots as operational_metrics,
)
from backend.core import (
    request_context as rc,
)
from backend.core import (
    request_telemetry as web_vitals_store,
)
from backend.core._duckdb_status import refresh_config_status
from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
from backend.core.metadata import slow_queries, usage_log, usage_log_db
from backend.core.metadata.usage_log import clear_usage_log
from backend.core.query_instrumentation import (
    InstrumentedDuckDBConnection,
    _InstrumentedRecordReader,
    _InstrumentedResult,
    _probe_duckdb_memory,
    _safe_weakref,
)
from backend.core.request_context import RequestContext, _enforce_service_access
from backend.core.request_telemetry import RequestTelemetry
from backend.main import app
from backend.repositories._base import origin_latency_us_expr
from backend.utils.date_utils import iso_z
from backend.utils.remote_access import TimeBounds
from backend.utils.telemetry import get_page_load_id
from backend.utils.time_window import resolve_window
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
        assert metric_expressions.origin_latency_us(set()) == "NULL"
        assert metric_expressions.origin_latency_us({"url", "pop"}) == "NULL"

    def test_neither_with_absent_none_signals_skip(self) -> None:
        assert metric_expressions.origin_latency_us(set(), absent=None) is None
        assert metric_expressions.origin_latency_us({"url"}, absent=None) is None

    def test_present_branches_ignore_absent_value(self) -> None:
        assert (
            metric_expressions.origin_latency_us({"ottfb", "ttfb"}, absent=None)
            == 'COALESCE("ottfb", "ttfb" * 1000000.0)'
        )
        assert metric_expressions.origin_latency_us({"ottfb"}, absent=None) == '"ottfb"'
        assert metric_expressions.origin_latency_us({"ttfb"}, absent=None) == '"ttfb" * 1000000.0'


class TestBaseSeedDelegation:
    def test_origin_latency_us_expr_matches_catalog(self) -> None:
        for cols in ({"ottfb", "ttfb"}, {"ottfb"}, {"ttfb"}, set(), {"url"}):
            assert origin_latency_us_expr(cols) == metric_expressions.origin_latency_us(cols, absent="NULL")
        assert origin_latency_us_expr(set()) == "NULL"


class TestNullDefaultHelpersGoldenSql:
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
        assert s("requests") == "COALESCE(SUM(requests), 0)" and s("num") == "COALESCE(SUM(num), 0)"

    def test_network_total_bytes(self) -> None:
        s = metric_expressions.sum_or_zero
        assert f"{s('resp_bytes')} AS total_bytes" == "COALESCE(SUM(resp_bytes), 0) AS total_bytes"
        assert f"{s('bytes')} AS total_bytes" == "COALESCE(SUM(bytes), 0) AS total_bytes"

    def test_value_live_tail_exprs(self) -> None:
        s, hit = metric_expressions.sum_or_zero, metric_expressions.HIT_STATES
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
        self._rows, self.rowcount = list(rows), rowcount

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakePgConnection:
    def __init__(self, rows=(), rowcount=0):
        self.calls, self._rows, self._rowcount = [], rows, rowcount

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return _FakePgCursor(self._rows, self._rowcount)

    def close(self):
        pass


def test_record_and_get_global_metric_roundtrip():
    now = datetime.now(UTC)
    metric_snapshots.record_snapshot("cpu_load_1m", 1.5, ts=iso_z(now - timedelta(seconds=10)))
    metric_snapshots.record_snapshot("cpu_load_1m", 2.0, ts=iso_z(now))
    out = _get_history("cpu_load_1m", since=now - timedelta(hours=1))
    assert len(out) == 2 and {r["value"] for r in out} == {1.5, 2.0}
    assert all("ts" in r and "value" in r for r in out)


def test_per_service_and_per_task_filtering():
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 12.3, service_id="svc-a")
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 45.6, service_id="svc-b")
    metric_snapshots.record_snapshot("cron_duration_ms", 1000.0, service_id="svc-a", task="sync")
    metric_snapshots.record_snapshot("cron_duration_ms", 2000.0, service_id="svc-a", task="commit")
    since = datetime.now(UTC) - timedelta(hours=1)
    assert [r["value"] for r in _get_history("pool_wait_p95_ms", since=since, service_id="svc-a")] == [12.3]
    assert [r["value"] for r in _get_history("pool_wait_p95_ms", since=since, service_id="svc-b")] == [45.6]
    assert [r["value"] for r in _get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")] == [
        1000.0
    ]


def test_global_query_excludes_per_service_rows():
    metric_snapshots.record_snapshot("cpu_load_1m", 0.5)
    metric_snapshots.record_snapshot("cpu_load_1m", 99.0, service_id="svc-x")
    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert [r["value"] for r in out] == [0.5]


def test_get_batch_keys_by_scope():
    metric_snapshots.record_snapshot("cpu_load_1m", 0.7)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 5.0, service_id="svc-a")
    metric_snapshots.record_snapshot("cron_duration_ms", 800.0, service_id="svc-a", task="sync")
    batch = metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1))
    assert {"cpu_load_1m", "pool_wait_p95_ms|svc-a", "cron_duration_ms|svc-a|sync"} <= set(batch)
    assert len(batch["cpu_load_1m"]) == 1 and batch["cpu_load_1m"][0]["value"] == 0.7


def test_since_clamp_excludes_old_rows():
    metric_snapshots.record_snapshot("cpu_load_1m", 1.1, ts=iso_z(datetime.now(UTC) - timedelta(days=10)))
    metric_snapshots.record_snapshot("cpu_load_1m", 2.2)
    out = _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))
    assert [r["value"] for r in out] == [2.2]


def test_purge_old_drops_only_aged_rows():
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0, ts=iso_z(datetime.now(UTC) - timedelta(days=1)))
    metric_snapshots.record_snapshot("cpu_load_1m", 9.9, ts=iso_z(datetime.now(UTC) - timedelta(days=60)))
    assert metric_snapshots.purge_old(retention_days=30) == 1
    assert [r["value"] for r in _get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(days=90))] == [1.0]


def test_purge_zero_or_negative_retention_is_noop():
    metric_snapshots.record_snapshot("cpu_load_1m", 1.0)
    assert metric_snapshots.purge_old(retention_days=0) == 0 and metric_snapshots.purge_old(retention_days=-5) == 0
    assert len(_get_history("cpu_load_1m", since=datetime.now(UTC) - timedelta(hours=1))) == 1


def test_get_batch_returns_empty_before_first_write():
    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("cpu_load_1m", since=since) == [] and metric_snapshots.get_batch(since=since) == {}


def test_idempotent_insert_overwrites_same_pk_per_service():
    ts = iso_z(datetime.now(UTC))
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 1.0, service_id="svc-a", ts=ts)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 2.0, service_id="svc-a", ts=ts)
    out = _get_history("pool_wait_p95_ms", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-a")
    assert len(out) == 1 and out[0]["value"] == 2.0


def test_postgres_backend_is_shared_for_metric_writes_and_reads(monkeypatch):
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
    assert "ON CONFLICT" in write_con.calls[0][0] and write_con.calls[0][1][2] == "svc-a"
    assert read_con.calls[0][1] == ("2025-12-31T00:00:00Z",)


# ── Merged from tests/core/test_metrics.py ──
def test_every_metric_type_has_a_definition_or_is_specific_status():
    for m in MetricType:
        assert m.value in METRIC_DEFINITIONS or m == MetricType.SPECIFIC_STATUS


def test_returns_aggregation_for_simple_metric():
    assert get_metric_sql(MetricType.REQUESTS.value) == "count(*)"


def test_returns_aggregation_for_5xx_rate():
    sql = get_metric_sql("5xx_rate")
    assert "100.0" in sql and "NULLIF(count(*), 0)" in sql


def test_unknown_metric_falls_back_to_count():
    assert get_metric_sql("totally_made_up_metric") == "count(*)"


def test_5xx_uses_where_clause_optimization():
    sql = get_metric_sql(MetricType.ERRORS_5XX.value, table_name="logs")
    assert "WHERE status >= 500" in sql and "CASE WHEN" not in sql and "FROM logs" in sql


def test_4xx_uses_where_clause_optimization():
    sql = get_metric_sql(MetricType.ERRORS_4XX.value, table_name="logs")
    assert "WHERE status BETWEEN 400 AND 499" in sql and "CASE WHEN" not in sql


def test_ttfb_filters_on_not_null():
    sql = get_metric_sql(MetricType.TTFB.value, table_name="logs")
    assert "ottfb IS NOT NULL" in sql and "FROM logs" in sql


def test_requests_with_table_wraps_in_select():
    sql = get_metric_sql(MetricType.REQUESTS.value, table_name="logs_svc")
    assert sql.startswith("SELECT ") and "FROM logs_svc" in sql and "count(*)" in sql


def test_specific_status_renders_status_list_into_select():
    sql = get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[404, 403], table_name="logs")
    assert "WHERE 1=1" in sql and "status IN (404, 403)" in sql and "FROM logs" in sql


def test_specific_status_rate_uses_aggregation_form():
    sql = get_metric_sql("specific_status_rate", status_codes=[500])
    assert (
        "sum(CASE WHEN 1=1  AND status IN (500) THEN 1 ELSE 0 END)" in sql
        and "100.0" in sql
        and "NULLIF(count(*), 0)" in sql
    )


def test_specific_status_empty_codes_renders_no_filter():
    assert "IN" not in get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[])


def test_specific_status_ignores_non_int_codes():
    sql = get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[200, "drop table users", 404])  # type: ignore[list-item]
    assert "200" in sql and "404" in sql and "drop" not in sql.lower()


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
    t.execute(get_metric_sql(metric, table_name="logs")).fetchone()


def test_specific_status_generated_sql_executes(t):
    rows = t.execute(
        get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[200, 404], table_name="logs")
    ).fetchone()
    assert rows[0] == 2


# ── Merged from tests/core/test_request_context.py ──
pytestmark_for_tenancy = pytest.mark.security_regression


@pytest.mark.security_regression
def test_admin_request_with_service_id_passes():
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=None))
    assert _enforce_service_access(request, "svc-123") == "svc-123"


@pytest.mark.security_regression
def test_admin_request_without_service_id_raises_400():
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=None))
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, None)
    assert exc.value.status_code == 400 and exc.value.detail["error"] == "no_service"


@pytest.mark.security_regression
def test_scoped_analyst_with_authorized_service_passes():
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=SimpleNamespace(service_ids=["svc-1", "svc-2"])))
    assert _enforce_service_access(request, "svc-1") == "svc-1"


@pytest.mark.security_regression
def test_scoped_analyst_with_unauthorized_service_raises_403():
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=SimpleNamespace(service_ids=["svc-1"])))
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, "svc-other")
    assert exc.value.status_code == 403 and exc.value.detail["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_scoped_analyst_without_service_id_defaults_to_first_allowed():
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=SimpleNamespace(service_ids=["svc-7", "svc-8"])))
    assert _enforce_service_access(request, None) in {"svc-7", "svc-8"}


@pytest.mark.security_regression
def test_scoped_analyst_with_empty_invite_raises_400():
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=SimpleNamespace(service_ids=[])))
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, None)
    assert exc.value.status_code == 400


def test_request_context_carries_required_fields():
    ctx = RequestContext(
        service_id="svc-1",
        source={"name": "svc-1", "endpoint_url": "http://localhost"},
        _con_override=MagicMock(),
        telemetry=RequestTelemetry("GET", "/api/x"),
        analyst_session=None,
    )
    assert ctx.service_id == "svc-1" and ctx.source["name"] == "svc-1"
    assert ctx.con is not None and ctx.telemetry is not None and ctx.analyst_session is None
    assert ctx.cached_temps == {} and ctx.read_only is True


def test_cached_temps_are_per_instance():
    a = RequestContext(
        service_id="svc", source={"name": "svc"}, _con_override=MagicMock(), telemetry=RequestTelemetry("GET", "/")
    )
    b = RequestContext(
        service_id="svc", source={"name": "svc"}, _con_override=MagicMock(), telemetry=RequestTelemetry("GET", "/")
    )
    a.cached_temps["window:1h"] = "tmp_1234"
    assert "window:1h" not in b.cached_temps


def test_resolve_source_returns_source_dict_for_known_service():
    fake_source = {"name": "svc-x", "endpoint_url": "http://localhost"}
    with patch("backend.core.duckdb.get_source_for_service", return_value=fake_source):
        assert rc._resolve_source("svc-x") is fake_source


def test_resolve_source_raises_400_for_unknown_service():
    with patch("backend.core.duckdb.get_source_for_service", return_value=None), pytest.raises(HTTPException) as exc:
        rc._resolve_source("svc-missing")
    assert exc.value.status_code == 400 and exc.value.detail["no_service"] is True


def _make_app_with_ctx_route() -> FastAPI:
    sub_app = FastAPI()
    fake_source = {"name": "svc-1", "endpoint_url": "http://localhost"}

    @sub_app.middleware("http")
    async def install_session(request: FRequest, call_next):
        sid_header = request.headers.get("x-test-session-services")
        request.state.analyst_session = (
            SimpleNamespace(service_ids=[s.strip() for s in sid_header.split(",") if s.strip()])
            if sid_header is not None
            else None
        )
        return await call_next(request)

    @sub_app.get("/api/test")
    def route(ctx: RequestContext = Depends(rc.build_request_context)):
        return {"service_id": ctx.service_id, "read_only": ctx.read_only, "is_admin": ctx.analyst_session is None}

    sub_app.state._fake_source = fake_source
    return sub_app


def _call_ctx_route(**get_kwargs):
    sub_app = _make_app_with_ctx_route()
    with (
        patch("backend.core.request_context._resolve_source", return_value=sub_app.state._fake_source),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        return TestClient(sub_app).get("/api/test", **get_kwargs)


@pytest.mark.security_regression
def test_route_admin_request_with_explicit_service_succeeds(monkeypatch):
    r = _call_ctx_route(params={"service": "svc-1"})
    assert r.status_code == 200 and r.json() == {"service_id": "svc-1", "read_only": True, "is_admin": True}


@pytest.mark.security_regression
def test_route_analyst_request_with_unauthorized_service_403s(monkeypatch):
    r = _call_ctx_route(params={"service": "svc-OTHER"}, headers={"x-test-session-services": "svc-1"})
    assert r.status_code == 403 and r.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_route_read_only_cannot_be_overridden_by_query_param(monkeypatch):
    r = _call_ctx_route(params={"service": "svc-1", "read_only": "false"})
    assert r.status_code == 200 and r.json()["read_only"] is True


def _install_inspectable_pool(monkeypatch, service_key: str):
    pool = duckdb_pool._Pool(service_key=service_key, max_size=2)
    mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_con.execute.return_value.fetchall.return_value = []
    pool._idle.put_nowait(mock_con)
    pool._in_use = 1
    monkeypatch.setattr(duckdb_pool, "_pool_enabled", lambda: True)
    monkeypatch.setitem(duckdb_pool._pools, service_key, pool)
    monkeypatch.setattr(duckdb_pool._Pool, "_prepare_checkout", lambda self, con, src: con)
    return pool, mock_con


def _drive_request_context_to_yield(service_key: str):
    request = MagicMock()
    request.state = SimpleNamespace(analyst_session=None)
    request.method = "GET"
    request.url.path = "/api/test"
    gen = rc.build_request_context(request, service_id=service_key)
    return gen, next(gen)


def test_client_cancel_returns_connection_to_pool(monkeypatch):
    pool, mock_con = _install_inspectable_pool(monkeypatch, "svc-cancel")
    with patch(
        "backend.core.request_context._resolve_source",
        return_value={"name": "svc-cancel", "endpoint_url": "http://localhost"},
    ):
        gen, ctx = _drive_request_context_to_yield("svc-cancel")
        _ = ctx.con
        assert pool._in_use == 1 and pool._idle.qsize() == 0
        gen.close()
    assert pool._in_use == 1 and pool._idle.qsize() == 1 and pool._discarded_total == 0
    mock_con.close.assert_not_called()


def test_real_exception_discards_connection(monkeypatch):
    pool, mock_con = _install_inspectable_pool(monkeypatch, "svc-err")
    with patch(
        "backend.core.request_context._resolve_source",
        return_value={"name": "svc-err", "endpoint_url": "http://localhost"},
    ):
        gen, ctx = _drive_request_context_to_yield("svc-err")
        _ = ctx.con
        assert pool._in_use == 1
        with pytest.raises(RuntimeError, match="boom"):
            gen.throw(RuntimeError("boom"))
    assert pool._discarded_total == 1 and pool._in_use == 0 and pool._idle.qsize() == 0
    mock_con.close.assert_called_once()


def test_prewarm_con_acquires_connection_off_the_event_loop():
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
    assert seen["thread"] != loop_thread and ctx.con is ctx._holder.con


def test_prewarm_con_cancellation_still_completes_checkout():
    release, entered = threading.Event(), threading.Event()

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
        asyncio.get_running_loop().call_later(0.1, release.set)
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run())
    assert ctx._holder.con is not None


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _ctx(time_bounds: TimeBounds | None, analyst: object | None) -> RequestContext:
    return make_test_request_context(
        service_id="svc", source={"name": "svc", "service_id": "svc"}, session=analyst, time_bounds=time_bounds
    )


def _req(**kw):
    return SimpleNamespace(**{"range_token": None, "anchor": None, "start_time": None, "end_time": None, **kw})


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
    assert _dt(start) >= invite_start and _dt(end) <= anchor_dt


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
            assert info[0] in (0, 1, 2)
        finally:
            con.close()

    return source, replace_rows, cron, tmp_path


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_reads_inlined_rows_ignoring_local_parquet_and_native_file(durable_metrics):
    source, replace_rows, _, path = durable_metrics
    first, last = datetime(2026, 9, 7, 12, tzinfo=UTC), datetime(2026, 9, 7, 19, tzinfo=UTC)
    replace_rows([first, last])
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
    assert observed["earliest_log_at"] == first.isoformat() and observed["latest_log_at"] == last.isoformat()
    assert observed["local_rows"] == 5 and not (path / "must-not-open.duckdb").exists()
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
    assert observed["request"]["total_rows"] == 1 and observed["request"]["latest_log_at"] == latest.isoformat()


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_reads_durable_rum_extents(durable_metrics):
    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, 12, tzinfo=UTC)])
    con = duckdb.connect(":memory:")
    try:
        assert _ducklake_attach(con, source, read_only=False)
        vitals_table, errors_table = (
            ducklake_table_name(source, "client_vitals"),
            ducklake_table_name(source, "client_errors"),
        )
        for table in (vitals_table, errors_table):
            con.execute(
                f'CREATE TABLE IF NOT EXISTS lake."{table}" (timestamp TIMESTAMPTZ, req_id VARCHAR, cid VARCHAR)'
            )
            con.execute(f'DELETE FROM lake."{table}"')
        con.execute(
            f"INSERT INTO lake.\"{vitals_table}\" VALUES (TIMESTAMPTZ '2026-09-08 01:00:00+00', '', 'vital-1'), (TIMESTAMPTZ '2026-09-08 02:00:00+00', '', 'vital-2')"
        )
        con.execute(f"INSERT INTO lake.\"{errors_table}\" VALUES (TIMESTAMPTZ '2026-09-08 03:00:00+00', '', 'error-1')")
    finally:
        con.close()

    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["rum"] == {"latest_log_at": "2026-09-08T03:00:00+00:00", "total_rows": 3, "last_sync_at": None}
    assert config.get_status(source["name"])["rum"] == observed["rum"]


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_refreshes_rum_sync_time_from_latest_cron_run(durable_metrics):
    source, replace_rows, cron, _ = durable_metrics
    status = config.get_status(source["name"])
    status["rum"]["last_sync_at"] = "2026-09-07T00:00:00Z"
    config.update_status(source["name"], status)
    cron["rum_sync"] = {"started_at": "2026-09-12T15:00:00Z"}
    replace_rows([datetime(2026, 9, 7, 12, tzinfo=UTC)])
    assert request_metrics.refresh_durable_request_metrics(source)["rum"]["last_sync_at"] == "2026-09-12T15:00:00Z"


@pytest.mark.usefixtures("_no_real_kill")
def test_observation_accepts_backward_extent_reset_and_idle_cron(durable_metrics):
    source, replace_rows, cron, _ = durable_metrics
    latest, older = datetime(2026, 9, 7, tzinfo=UTC), datetime(2026, 9, 2, tzinfo=UTC)
    replace_rows([latest])
    request_metrics.refresh_durable_request_metrics(source)
    replace_rows([older])
    assert request_metrics.refresh_durable_request_metrics(source)["latest_log_at"] == older.isoformat()
    replace_rows([])
    cron["log_discovery"]["started_at"] = "2026-09-08T18:01:00Z"
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["request"] == {"latest_log_at": None, "total_rows": 0, "last_sync_at": "2026-09-08T18:01:00Z"}
    assert observed["earliest_log_at"] is observed["latest_log_at"] is None and observed["local_rows"] == 3


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
    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, tzinfo=UTC)])
    monkeypatch.setattr(
        "backend.core.duckdb.get_sync_status", MagicMock(side_effect=AssertionError("local/stale stats path called"))
    )
    monkeypatch.setattr("backend.core.iceberg.get_table_info", lambda src: {})
    monkeypatch.setattr("backend.repositories.usage.get_edge_ratio", lambda *a: (None, None))
    refresh_config_status(source["name"], include_top_values=False)
    assert config.get_status(source["name"])["request"]["total_rows"] == 1
    replace_rows([])
    refresh_config_status(source["name"], include_top_values=False)
    status = config.get_status(source["name"])
    assert (
        status["request"]["total_rows"] == 0 and status["request"]["latest_log_at"] is status["latest_log_at"] is None
    )


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
    interrupted: list[bool] = []

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
    assert interrupted and config.get_status(source["name"]) == previous


@pytest.mark.usefixtures("_no_real_kill")
def test_real_observation_is_persisted_before_sse_publication(durable_metrics, monkeypatch):
    source, replace_rows, _, _ = durable_metrics
    latest = datetime(2026, 9, 7, tzinfo=UTC)
    replace_rows([latest])
    published = []

    def publish(service_id, payload):
        persisted = config.get_status(service_id)
        assert persisted["request"] == payload["request"] and persisted["latest_log_at"] == payload["latest_log_at"]
        published.append(payload["request"])

    monkeypatch.setattr(observer_mod, "compute_sync_status_cached", config.get_status)
    monkeypatch.setattr(observer_mod.publisher, "publish", publish)
    observer = observer_mod.RequestMetricsObserver()
    observer.reconcile()
    observer.reconcile()
    assert len(published) == 1 and published[0]["latest_log_at"] == latest.isoformat()
    replace_rows([])
    observer.reconcile()
    assert len(published) == 2 and published[-1]["total_rows"] == 0 and published[-1]["latest_log_at"] is None


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
    assert description == "Celery broker queue depth" and unit == "{messages}"


@pytest.mark.usefixtures("_no_real_kill")
def test_record_keeps_distinct_label_sets(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)
    operational_metrics.record("ingest_ledger_rows", 3, status="discovered")
    operational_metrics.record("ingest_ledger_rows", 2, status="claimed")
    gauge, _, _ = meter.gauges["fla_ingest_ledger_rows"]
    observations = gauge.callbacks[0](None)
    assert {(item.value, item.attributes["status"]) for item in observations} == {(3.0, "discovered"), (2.0, "claimed")}


@pytest.mark.usefixtures("_no_real_kill")
def test_high_scale_metrics_are_exported_with_domain_labels(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)
    operational_metrics.record("high_scale_publication_lag_seconds", 12.5, service_id="svc", domain="request")
    gauge, description, unit = meter.gauges["fla_high_scale_publication_lag_seconds"]
    observation = gauge.callbacks[0](None)[0]
    assert observation.value == 12.5 and observation.attributes == {"domain": "request", "service_id": "svc"}
    assert description == "Age of the oldest pending high-scale publication" and unit == "s"


@pytest.mark.usefixtures("_no_real_kill")
def test_telemetry_schema_and_helpers(monkeypatch, tmp_path):
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    con = usage_log_db.get_con("test_service")
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
        ).fetchall()
    ]
    assert "telemetry_queries" in tables and "telemetry_sections" in tables
    usage_log.log_telemetry_query(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        engine="duckdb",
        sql="SELECT 1",
        duration_ms=45.2,
    )
    usage_log.log_telemetry_section(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        section_name="dashboard.aggregates",
        duration_ms=12.5,
    )
    telemetry = usage_log.get_page_telemetry("test_service", "page-123")
    assert len(telemetry["queries"]) == 1 and telemetry["queries"][0]["sql_query"] == "SELECT 1"
    assert telemetry["queries"][0]["engine"] == "duckdb" and telemetry["queries"][0]["duration_ms"] == 45.2
    assert (
        len(telemetry["sections"]) == 1
        and telemetry["sections"][0]["section_name"] == "dashboard.aggregates"
        and telemetry["sections"][0]["duration_ms"] == 12.5
    )


@pytest.mark.usefixtures("_no_real_kill")
def test_page_load_id_propagation_middleware():
    response = TestClient(app).get("/api/health", headers={"X-Page-Load-ID": "test-page-999"})
    assert response.status_code == 200 and get_page_load_id() is None


@pytest.fixture
def _no_real_kill(monkeypatch):
    calls = []
    monkeypatch.setattr(memory_guard.os, "kill", lambda pid, sig: calls.append((pid, sig)))
    return calls


def _set_rss(monkeypatch, value):
    monkeypatch.setattr("backend.core.duckdb.current_rss_bytes", lambda: value)


@pytest.mark.usefixtures("_no_real_kill")
def test_disabled_by_default(monkeypatch, _no_real_kill):
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    _set_rss(monkeypatch, 50_000 * 1024 * 1024)
    assert memory_guard.maybe_graceful_restart() is False and _no_real_kill == []


@pytest.mark.usefixtures("_no_real_kill")
def test_below_threshold_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 5000 * 1024 * 1024)
    assert memory_guard.maybe_graceful_restart() is False and _no_real_kill == []


@pytest.mark.usefixtures("_no_real_kill")
def test_at_or_above_threshold_sends_sigterm(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, 9500 * 1024 * 1024)
    assert memory_guard.maybe_graceful_restart() is True and len(_no_real_kill) == 1
    pid, sig = _no_real_kill[0]
    assert sig == signal.SIGTERM and pid > 0


@pytest.mark.usefixtures("_no_real_kill")
def test_unreadable_rss_does_not_restart(monkeypatch, _no_real_kill):
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    _set_rss(monkeypatch, None)
    assert memory_guard.maybe_graceful_restart() is False and _no_real_kill == []


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
    sink = tmp_path / "nested" / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)
    web_vitals_store.append_sample({"name": "LCP", "value": 1200.0, "rating": "good"})
    web_vitals_store.append_sample({"name": "CLS", "value": 0.05, "rating": "good"})
    lines = sink.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2 and json.loads(lines[0])["name"] == "LCP" and json.loads(lines[1])["name"] == "CLS"


@pytest.mark.usefixtures("_no_real_kill")
def test_append_sample_never_raises_on_io_error(monkeypatch, tmp_path):
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", blocker / "child" / "wv.jsonl")
    web_vitals_store.append_sample({"name": "LCP", "value": 1.0, "rating": "good"})


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
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", tmp_path / "wv.jsonl")
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0.00005")
    for i in range(4):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})
    active, backup = tmp_path / "wv.jsonl", web_vitals_store.rotated_path()
    assert active.exists() and backup.exists()
    assert len(active.read_text(encoding="utf-8").strip().splitlines()) < 4 and not (tmp_path / "wv.jsonl.2").exists()


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
        assert attributes["site"] == "elevation-dev-usc1" and attributes["namespace"] == "test-namespace"


@pytest.mark.usefixtures("_reset_module")
def test_setup_sdk_otlp_installs_otlp_exporters(monkeypatch):
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
    "missing_endpoint", ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]
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
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "0")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    assert request_telemetry._otel_enabled() is False


@pytest.mark.usefixtures("_reset_module")
def test_get_tracer_returns_a_tracer():
    assert request_telemetry.get_tracer() is not None


@pytest.mark.usefixtures("_reset_module")
def test_setup_sdk_is_idempotent():
    for _ in range(3):
        request_telemetry._setup_sdk()


@pytest.mark.usefixtures("_reset_module")
def test_request_lifecycle_in_test_mode():
    ctx = request_telemetry.RequestTelemetry("GET", "/api/dashboard/aggregates")
    ctx.start_request()
    ctx.start_request()
    ctx.end_request(status_code=200)
    ctx.end_request()


@pytest.mark.usefixtures("_reset_module")
def test_thread_wait_histogram_constructed_once():
    assert request_telemetry.thread_wait_histogram() is request_telemetry.thread_wait_histogram()


@pytest.mark.usefixtures("_reset_module")
def test_start_request_reuses_ambient_span_instead_of_opening_a_second_one():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    ambient_span = provider.get_tracer("ambient").start_span("GET /api/ambient-example")
    with (
        trace.use_span(ambient_span, end_on_exit=False),
        patch("backend.core.request_telemetry.get_tracer") as mock_get_tracer,
    ):
        ctx = request_telemetry.RequestTelemetry("GET", "/api/ambient-example")
        ctx.start_request()
        ctx.end_request(status_code=200)
    mock_get_tracer.assert_not_called()
    assert exporter.get_finished_spans() == ()
    ambient_span.end()
    (finished,) = exporter.get_finished_spans()
    assert finished.name == "GET /api/ambient-example"
    assert (
        finished.attributes is not None
        and finished.attributes.get("http.status_code") == 200
        and finished.attributes.get("app.is_cached") is False
    )


@pytest.mark.usefixtures("_reset_module")
def test_thread_wait_histogram_records_observation_via_meter_provider():
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    saved_histogram, saved_meter_get = request_telemetry._thread_wait_histogram, request_telemetry.get_meter
    try:
        request_telemetry._thread_wait_histogram = None
        request_telemetry.get_meter = lambda: provider.get_meter("test-meter")  # type: ignore[assignment]
        h = request_telemetry.thread_wait_histogram()
        h.record(12.5, {"service": "svc-a", "outcome": "reused"})
        h.record(45.0, {"service": "svc-a", "outcome": "created"})
        h.record(125.0, {"service": "svc-b", "outcome": "timeout"})
        data = reader.get_metrics_data()
        assert data is not None
        histogram_metrics = [m for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics]
        assert any("thread_wait" in m.name.lower() or "wait" in m.name.lower() for m in histogram_metrics)
        total_observed = sum(dp.count for m in histogram_metrics for dp in getattr(m.data, "data_points", []))
        assert total_observed >= 3
    finally:
        request_telemetry._thread_wait_histogram = saved_histogram
        request_telemetry.get_meter = saved_meter_get  # type: ignore[assignment]
        ot_metrics  # noqa: B018


# ── Merged from tests/core/test_query_instrumentation.py ──
@pytest.fixture
def duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    yield con
    try:
        con.close()
    except Exception:
        pass


def test_finish_is_idempotent_no_double_deregister():
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result = _InstrumentedResult(MagicMock(), qid=7, con_ref=None)
        for _ in range(3):
            result._finish(None, probe_memory=False)
    assert mock_dereg.call_count == 1 and mock_dereg.call_args.args == (7, None)


def test_arrow_reader_defers_deregister_until_exhausted(duck):
    duck.execute("CREATE TABLE t AS SELECT i FROM range(50_000) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="defer_svc")
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        reader = con.execute("SELECT * FROM t").arrow()
        assert mock_dereg.call_count == 0 and isinstance(reader, _InstrumentedRecordReader)
        assert sum(b.num_rows for b in reader) == 50_000 and mock_dereg.call_count == 1


def test_fetch_record_batch_defers_until_drained(duck):
    duck.execute("CREATE TABLE t AS SELECT i FROM range(1_000) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="frb_svc")
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        reader = con.execute("SELECT * FROM t").fetch_record_batch(100)
        assert mock_dereg.call_count == 0
        assert sum(b.num_rows for b in reader) == 1_000 and mock_dereg.call_count == 1


@pytest.mark.parametrize("method", ["fetchall", "fetchone", "fetchnumpy"])
def test_terminal_method_deregisters_immediately(duck, method):
    duck.execute("CREATE TABLE t AS SELECT i FROM range(10) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="term_svc")
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        getattr(con.execute("SELECT i FROM t"), method)()
        assert mock_dereg.call_count == 1 and mock_dereg.call_args.args[1] is None


def test_execute_exception_deregisters_with_error(duck):
    con = InstrumentedDuckDBConnection(duck, service_id="err_svc")
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM table_that_does_not_exist_xyz")
        assert mock_dereg.call_count == 1 and isinstance(mock_dereg.call_args.args[1], BaseException)


def test_finish_safe_when_underlying_con_gc_collected():
    class _WeakRefable:
        def cursor(self):
            raise AssertionError("probe ran on a dead connection")

    raw = _WeakRefable()
    ref = _safe_weakref(raw)
    assert ref is not None and ref() is raw
    result = _InstrumentedResult(MagicMock(), qid=42, con_ref=ref)
    del raw
    gc.collect()
    assert ref() is None
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result._finish(None, probe_memory=True)
    assert mock_dereg.call_count == 1 and mock_dereg.call_args.kwargs.get("peak_memory_mb") is None


def test_safe_weakref_falls_back_to_strong_ref_for_non_weakrefable():
    not_weakrefable = 12345
    with pytest.raises(TypeError):
        weakref.ref(not_weakrefable)
    ref = _safe_weakref(not_weakrefable)
    assert ref is not None and ref() == 12345


def _probe_with_row(row):
    fake_con, fake_cursor = MagicMock(), MagicMock()
    fake_con.cursor.return_value = fake_cursor
    fake_cursor.execute.return_value.fetchone.return_value = row
    return _probe_duckdb_memory(fake_con)


def test_probe_returns_none_on_malformed_pragma_row():
    assert _probe_with_row(("not-a-number",)) is None


def test_probe_returns_none_on_none_row():
    assert _probe_with_row(None) is None


def test_probe_returns_none_on_null_byte_value():
    assert _probe_with_row((None,)) is None


def test_probe_returns_none_when_cursor_raises():
    fake_con = MagicMock()
    fake_con.cursor.side_effect = RuntimeError("no such function: duckdb_memory")
    assert _probe_duckdb_memory(fake_con) is None


# ── Merged from tests/core/test_slow_queries_optimizations.py ──
def test_usage_log_purging_and_trigger_restricton(tmp_path, monkeypatch):
    sid = "test_purge_svc"
    usage_log.clear_usage_log(sid)
    old_ts = "2020-05-01T10:00:00Z"
    now_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    now_hour, now_ts_1, now_ts_2 = now_dt.strftime("%Y-%m-%dT%H"), iso_z(now_dt), iso_z(now_dt + timedelta(minutes=15))

    con = usage_log_db.get_con(sid)
    try:
        con.execute(
            "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name) VALUES (?, ?, ?, ?, ?, ?)",
            (old_ts, sid, "A", "CDN", 100, "api.sync"),
        )
        con.execute(
            "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name) VALUES (?, ?, ?, ?, ?, ?)",
            (now_ts_2, sid, "A", "RECONCILE_A", 50, "fastly.reconciliation"),
        )
        con.execute(
            "INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (sid, "2020-05-01T10", "A", "CDN", 100, 0, old_ts),
        )
        con.execute(
            "INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (sid, now_hour, "A", "RECONCILE_A", 50, 0, now_ts_1),
        )
        con.commit()
        assert (
            con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0]
            == 150
        )
        usage_log.purge_usage_log(sid, retention_days=30)
        assert (
            con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0]
            == 150
        )
        usage_log.reconcile_fastly_stats(sid, [{"hour_iso": now_ts_1, "class_a": 0, "class_b": 0}])
        assert (
            con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0]
            == 100
        )
    finally:
        usage_log_db.close_all_connections()


def test_clear_usage_log_wipes_both_tables(tmp_path, monkeypatch):
    sid = "test_clear_svc"
    con = usage_log_db.get_con(sid)
    try:
        con.execute(
            "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name) VALUES (?, ?, ?, ?, ?, ?)",
            ("2026-05-01T10:00:00Z", sid, "A", "CDN", 100, "api.sync"),
        )
        con.execute(
            "INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (sid, "2026-05-01T10", "A", "CDN", 100, 0, "2026-05-01T10:00:00Z"),
        )
        con.commit()
        assert (
            con.execute("SELECT count FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 100
        )
    finally:
        usage_log_db.close_all_connections()

    clear_usage_log(sid)
    con = usage_log_db.get_con(sid)
    try:
        assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
        assert (
            con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 0
        )
    finally:
        usage_log_db.close_all_connections()


def _prime_slow_buffer(svc: str):
    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer[svc] = [{"query_id": "q1"}]


def test_flush_releases_postgres_thread_connection_after_success(monkeypatch):
    con, release = MagicMock(), MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    _prime_slow_buffer("svc-flush")
    slow_queries._flush_all()
    con.executemany.assert_called_once()
    con.commit.assert_called_once_with()
    release.assert_called_once_with()


def test_flush_releases_connection_when_write_fails(monkeypatch):
    con, release = MagicMock(), MagicMock()
    con.executemany.side_effect = RuntimeError("metadata unavailable")
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    _prime_slow_buffer("svc-flush-error")
    slow_queries._flush_all()
    release.assert_called_once_with()


def test_flush_releases_on_the_short_lived_worker_thread(monkeypatch):
    con, released_on = MagicMock(), []
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    monkeypatch.setattr(
        slow_queries, "release_thread_connection", lambda: released_on.append(threading.current_thread()), raising=False
    )
    _prime_slow_buffer("svc-thread")
    worker = threading.Thread(target=slow_queries._flush_all)
    worker.start()
    worker.join(timeout=2)
    assert not worker.is_alive() and released_on == [worker]


def test_flush_without_connection_keeps_best_effort_behavior(monkeypatch):
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", MagicMock(side_effect=RuntimeError("metadata unavailable")))
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    _prime_slow_buffer("svc-no-connection")
    slow_queries._flush_all()
    release.assert_called_once_with()
