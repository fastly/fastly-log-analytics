"""Focused unit tests for Round 13 High-Scale network, origin, insights, and log-accounting optimizations."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.network import (
    _network_json_cache,
    _network_memo,
    _network_model_cache,
    get_pop_health,
    get_transport_metrics,
    network_bundle,
    network_quality,
)
from backend.high_scale.origin import _origin_memo, origin_aggregates
from backend.models.analytics import NetworkBundleRequest, NetworkQualityRequest
from backend.repositories.utils.response_cache import invalidate_service_cache


class _FakeRealCHClient:
    def __init__(self, responder: Any) -> None:
        self._responder = responder
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._http = SimpleNamespace(_transport=SimpleNamespace())

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        p = dict(params or {})
        self.calls.append((sql, p))
        return self._responder(sql, p)


def _clear_caches() -> None:
    _network_memo.cache.clear()
    _network_model_cache.clear()
    _network_json_cache.clear()
    _origin_memo.cache.clear()


@pytest.mark.no_postgres
def test_pop_health_24h_uses_rollup_errors_and_skips_fact_tdigest(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    def responder(sql: str, _params: dict[str, Any]):
        if "UNION ALL" in sql:
            return [
                {
                    "kind": "rollup",
                    "pop": "SJC",
                    "req_count": 10_000,
                    "errors": 42,
                    "p50_rtt_us": 19_500.0,
                    "p95_ttfb_ms": 62.5,
                    "cache_hits": 0,
                    "bandwidth_bytes": 0,
                },
                {
                    "kind": "fact",
                    "pop": "SJC",
                    "req_count": 500,
                    "errors": None,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                    "cache_hits": 250,
                    "bandwidth_bytes": 50_000,
                },
            ]
        return []

    client = _FakeRealCHClient(responder)
    svc = HighScaleService(service_id="svc-pop-opt13", client=client, cursor_secret=b"s", request_watermark=None)
    resp = get_pop_health(svc, datetime(2026, 11, 24, 11, 0, tzinfo=UTC), datetime(2026, 11, 25, 11, 0, tzinfo=UTC))

    assert len(client.calls) == 1
    sql = client.calls[0][0]
    assert "event_timestamp >= toDateTime(" in sql
    assert "% 20 = 0" not in sql
    assert "cityHash64(event_id)" not in sql
    assert "quantileTDigest" not in sql
    assert "sum(errors)" in sql
    assert resp.data[0].requests == 10_000
    assert resp.data[0].errors == 42
    assert resp.data[0].p50_rtt_us == 19_500.0
    assert resp.data[0].p95_ttfb_ms == 62.5
    assert resp.data[0].cache_hit_rate == 50.0


@pytest.mark.no_postgres
def test_network_caches_bucket_to_minute_and_survive_ingest_invalidation(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    def responder(sql: str, _params: dict[str, Any]):
        if "FROM network_minute_dimensions" in sql:
            return [{"dimension": "country", "label": "US", "rtt_ms": 25.0, "reqs": 100}]
        if "cache_state" in sql:
            return [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 200
        if "proto_map" in sql:
            return [
                {
                    "total_reqs": 50,
                    "total_bytes": 5000,
                    "rtt_quants": [10000.0, 20000.0, 30000.0, 40000.0],
                    "avg_retrans": 0.1,
                    "avg_ploss_pct": 0.2,
                    "avg_del_rate_mbps": 10.0,
                    "proto_map": {"HTTP/2": 50},
                }
            ]
        return []

    client = _FakeRealCHClient(responder)
    svc = HighScaleService(service_id="svc-net-cache", client=client, cursor_secret=b"s", request_watermark=None)

    # First call at :15.150Z populates cache
    network_quality(svc, NetworkQualityRequest(), "2026-11-24T10:57:15.150Z", "2026-11-25T10:57:15.150Z")
    get_transport_metrics(svc, "2026-11-24T10:57:15.150Z", "2026-11-25T10:57:15.150Z")
    network_bundle(
        svc,
        NetworkBundleRequest(sections=["quality", "transport"]),
        "2026-11-24T10:57:15.150Z",
        "2026-11-25T10:57:15.150Z",
    )
    calls_after_cold = len(client.calls)

    # Routine 10s ingest invalidation must NOT wipe _network_memo / _network_model_cache
    invalidate_service_cache("svc-net-cache", scope="ingest")

    # Second call 2 seconds later within the same minute must hit cache with 0 new ClickHouse queries
    q_hit = network_quality(svc, NetworkQualityRequest(), "2026-11-24T10:57:17.890Z", "2026-11-25T10:57:17.890Z")
    t_hit = get_transport_metrics(svc, "2026-11-24T10:57:17.890Z", "2026-11-25T10:57:17.890Z")
    b_hit = network_bundle(
        svc,
        NetworkBundleRequest(sections=["quality", "transport"]),
        "2026-11-24T10:57:17.890Z",
        "2026-11-25T10:57:17.890Z",
    )
    assert len(client.calls) == calls_after_cold
    assert q_hit.is_cached is True
    assert t_hit.total_requests == 1000
    assert b_hit.is_cached is True

    # Explicit scope="all" invalidation MUST clear _network_memo, _network_model_cache, _network_json_cache
    invalidate_service_cache("svc-net-cache", scope="all")
    assert len(_network_memo.cache) == 0
    assert len(_network_model_cache) == 0
    assert len(_network_json_cache) == 0


@pytest.mark.no_postgres
def test_origin_aggregates_batches_dimensions_into_single_scan_and_minute_caches() -> None:
    _clear_caches()

    def responder(sql: str, _params: dict[str, Any]):
        if "origin:summary" in sql:
            return [
                {
                    "requests": 100,
                    "origin_5xx": 2,
                    "origin_4xx": 1,
                    "latency_q": [1000.0, 2500.0, 4000.0],
                    "latency_n": 100,
                    "ottlb_q": [1200.0, 3000.0],
                    "ottlb_n": 100,
                    "elapsed_q": [1500.0, 3500.0],
                    "elapsed_n": 100,
                    "total_bytes": 50_000,
                }
            ]
        if "origin:dimensions_batch" in sql:
            return [
                {
                    "dimension": "url",
                    "value": "/slow",
                    "requests": 50,
                    "origin_5xx": 1,
                    "latency_q": [1000.0, 5000.0, 9000.0],
                    "latency_n": 50,
                },
                {
                    "dimension": "status",
                    "value": "200",
                    "requests": 98,
                    "origin_5xx": 0,
                    "latency_q": [],
                    "latency_n": 0,
                },
                {
                    "dimension": "status",
                    "value": "503",
                    "requests": 2,
                    "origin_5xx": 2,
                    "latency_q": [],
                    "latency_n": 0,
                },
                {
                    "dimension": "edge",
                    "value": "true",
                    "requests": 70,
                    "origin_5xx": 1,
                    "latency_q": [1000.0, 2000.0, 3000.0],
                    "latency_n": 70,
                },
                {
                    "dimension": "edge",
                    "value": "false",
                    "requests": 30,
                    "origin_5xx": 1,
                    "latency_q": [1200.0, 2200.0, 3200.0],
                    "latency_n": 30,
                },
                {
                    "dimension": "pop",
                    "value": "SJC",
                    "requests": 100,
                    "origin_5xx": 2,
                    "latency_q": [1000.0, 2500.0, 4000.0],
                    "latency_n": 100,
                },
                {
                    "dimension": "oip",
                    "value": "10.0.0.1",
                    "requests": 100,
                    "origin_5xx": 2,
                    "latency_q": [1000.0, 2500.0, 4000.0],
                    "latency_n": 100,
                },
            ]
        return []

    client = _FakeRealCHClient(responder)
    svc = HighScaleService(service_id="svc-orig-opt13", client=client, cursor_secret=b"s", request_watermark=None)
    req = SimpleNamespace(
        filters={},
        slow_urls_limit=20,
        slow_urls_min_requests=10,
        pop_latency_limit=30,
        ip_health_limit=30,
    )
    sections = {"summary", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"}
    res = origin_aggregates(svc, req, "2026-11-24T10:57:10.100Z", "2026-11-25T10:57:10.100Z", sections=sections)

    dim_calls = [(sql, params) for sql, params in client.calls if "FROM origin_minute_dimensions" in sql]
    assert len(dim_calls) == 1
    dim_sql, dim_params = dim_calls[0]
    assert "origin:dimensions_batch" in dim_sql
    assert "Array(String)" not in dim_sql
    assert "LIMIT {" not in dim_sql
    assert "LIMIT 100 BY dimension" in dim_sql
    assert "dimension IN ({dim_0:String}" in dim_sql
    assert "toFloat64(ifNotFinite(latency_q[2], -1.0))" in dim_sql
    assert "dims" not in dim_params
    assert "per_dim_limit" not in dim_params
    assert {dim_params[k] for k in dim_params if k.startswith("dim_")} == {"url", "status", "edge", "pop", "oip"}
    assert res["slow_urls"]["rows"][0]["url"] == "/slow"
    assert res["status_codes"]["rows"][0] == {"status": 200, "count": 98, "pct": 98.0}
    assert res["path_breakdown"]["shielding_detected"] is True
    assert res["pop_latency"]["rows"][0]["pop"] == "SJC"
    assert res["ip_health"]["rows"][0]["error_pct"] == pytest.approx(2.0)

    # Survives ingest invalidation and hits cache within the same minute
    calls_before = len(client.calls)
    invalidate_service_cache("svc-orig-opt13", scope="ingest")
    res_hit = origin_aggregates(svc, req, "2026-11-24T10:57:45.900Z", "2026-11-25T10:57:45.900Z", sections=sections)
    assert len(client.calls) == calls_before
    assert res_hit["is_cached"] is True


@pytest.mark.no_postgres
def test_log_accounting_index_and_invalidation_scope() -> None:
    from backend.high_scale.postgres_control.control_plane import (
        _CONCURRENT_BILLING_INDEXES,
        SOURCE_OBJECTS_REQUEST_FLUSH_INDEX,
    )
    from backend.routers.admin import log_accounting as _la

    idx_map = dict(_CONCURRENT_BILLING_INDEXES)
    assert SOURCE_OBJECTS_REQUEST_FLUSH_INDEX in idx_map
    ddl = idx_map[SOURCE_OBJECTS_REQUEST_FLUSH_INDEX]
    assert "(service_id, object_key)" in ddl
    assert "INCLUDE (accepted_rows)" in ddl
    assert "WHERE domain = 'request' AND status NOT IN ('discovered', 'claimed')" in ddl

    _la._LOCAL_COUNTS_CACHE.set(("test-key",), {"2026-11-25T10": (10, 1)})
    invalidate_service_cache("svc-1", scope="ingest")
    assert _la._LOCAL_COUNTS_CACHE.get(("test-key",)) == {"2026-11-25T10": (10, 1)}

    invalidate_service_cache("svc-1", scope="all")
    assert _la._LOCAL_COUNTS_CACHE.get(("test-key",)) is None


@pytest.mark.no_postgres
def test_insights_evaluator_concurrency_caps() -> None:
    from backend.high_scale.insights import evaluator as _ev

    assert _ev._MAX_WORKERS == 2
    assert _ev._BACKGROUND_MAX_WORKERS == 2
    assert _ev._COLD_POOL._max_workers == 1
    assert _ev._REFRESH_POOL._max_workers == 1


@pytest.mark.no_postgres
def test_insights_url_batches_avoid_double_scan_subquery() -> None:
    from backend.high_scale.insights import batches as _b

    sql_perf, _ = _b._build_url_perf(
        {"url_latency_regression", "compression_drop"},
        present={"url", "elapsed", "content_type", "resp_bytes", "compression"},
        rollup_span=None,
        filter_clause="",
    )
    sql_cache, _ = _b._build_url_cache(
        {"cache_collapse", "pass_rate_surge"},
        present={"url", "cache_status"},
        rollup_span=None,
        filter_clause="",
    )
    assert "WHERE url IN (SELECT url FROM base" not in sql_perf
    assert "WHERE url IN (SELECT url FROM base" not in sql_cache


@pytest.mark.no_postgres
def test_usage_log_covering_index_hour_aligned_skip_and_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.core.metadata import fos_usage as _fu

    assert any(
        "idx_usage_service_ts_agg" in stmt
        and "(service_id, timestamp, operation_class, operation_type, count, bytes)" in stmt
        for stmt in _fu._SCHEMA
    )

    class _FakeCursor:
        def __init__(self, rows: list[dict[str, Any]]) -> None:
            self._rows = rows

        def fetchall(self) -> list[dict[str, Any]]:
            return self._rows

    class _FakeCon:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[Any]]] = []

        def execute(self, sql: str, params: list[Any]) -> _FakeCursor:
            self.calls.append((sql, list(params)))
            return _FakeCursor([{"operation_class": "A", "operation_type": "list_objects", "c": 42, "b": 1024}])

    _fu._USAGE_AGG_CACHE.clear()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    con = _FakeCon()

    # Hour-aligned start should skip start-boundary raw scan (only 1 usage_log branch, hour >= ?)
    rows1 = _fu._query_usage_log_aggregate_rollup(
        con, "svc-u1", "2026-11-24T12:00:00.000Z", "2026-11-25T12:15:00.000Z", ""
    )
    assert len(con.calls) == 1
    sql_aligned = con.calls[0][0]
    assert "hour >= ? AND hour < ?" in sql_aligned
    assert sql_aligned.count("FROM usage_log\n") == 1
    assert rows1 == [{"operation_class": "A", "operation_type": "list_objects", "c": 42, "b": 1024}]

    # Identical second call hits _USAGE_AGG_CACHE (dedupes /admin/usage-log page_size=50 + 500)
    rows2 = _fu._query_usage_log_aggregate_rollup(
        con, "svc-u1", "2026-11-24T12:00:00.000Z", "2026-11-25T12:15:00.000Z", ""
    )
    assert len(con.calls) == 1
    assert rows2 == rows1

    # Cache invalidation for service_id clears entry
    _fu._invalidate_usage_agg_cache("svc-u1")
    _fu._query_usage_log_aggregate_rollup(con, "svc-u1", "2026-11-24T12:00:00.000Z", "2026-11-25T12:15:00.000Z", "")
    assert len(con.calls) == 2

    # Non-hour-aligned start still uses 3-part UNION ALL (2 usage_log branches, hour > ?)
    _fu._query_usage_log_aggregate_rollup(con, "svc-u1", "2026-11-24T12:05:00.000Z", "2026-11-25T12:15:00.000Z", "")
    assert len(con.calls) == 3
    sql_unaligned = con.calls[2][0]
    assert "hour > ? AND hour < ?" in sql_unaligned
    assert sql_unaligned.count("FROM usage_log\n") == 2
    _fu._USAGE_AGG_CACHE.clear()
