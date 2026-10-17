"""Performance row models keep every producer's wire bytes (repository, rollup hit, high-scale)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from backend.high_scale import performance as hs_performance
from backend.models.performance import PerformanceAggregatesResponse
from backend.repositories._base import _safe_table
from backend.repositories.performance import get_performance_aggregates
from tests._helpers.wire import assert_sections_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _seed(con, src, n=120):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log["elapsed"] = 100_000 + (i * 7919) % 900_000
        log["ttfb"] = f"{(50_000 + (i * 1013) % 400_000) / 1_000_000:.6f}"
        log["ottfb"] = 20_000 + (i * 331) % 30_000
        log["ottlb"] = 60_000 + (i * 331) % 30_000
        log["url"] = f"/path-{i % 4}"
        log["asn"] = [7922, 15169][i % 2]
        log["ttl"] = [0, 5, 45, 3600, 90000][i % 5]
        log["cache"] = "HIT" if i % 3 else "MISS"
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def test_repository_live_wire_identical(in_memory_duckdb, test_service_source, monkeypatch):
    _seed(in_memory_duckdb, test_service_source)
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
    res = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    assert_sections_wire_identical(
        PerformanceAggregatesResponse, res, non_empty=("top_urls", "top_asns", "ttl_dist", "scatter", "waterfall")
    )


def test_repository_rollup_hit_wire_identical(in_memory_duckdb, test_service_source, monkeypatch):
    _seed(in_memory_duckdb, test_service_source)
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})

    def _hit(self, *_a, **kw):
        value = "/rolled" if kw.get("dimension") == "url" else 7922
        return {"rows": [{"value": value, "requests": 100, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}]}

    with patch("backend.repositories._base.QueryRunner.try_perf_latency_from_rollup", _hit):
        res = get_performance_aggregates(
            con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
        )
    assert res["approx"] is True
    assert_sections_wire_identical(PerformanceAggregatesResponse, res, non_empty=("top_urls", "top_asns"))


def test_repository_empty_schema_wire_identical(in_memory_duckdb):
    src = {"name": "perf_wire_missing", "service_id": "perf_wire_missing"}
    res = get_performance_aggregates(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={})
    assert_sections_wire_identical(PerformanceAggregatesResponse, res)


class _HsClient:
    def execute(self, sql, params=None):
        row = {"value": "/slow", "req_count": 9, "avg_ms": 12.5, "p50_ms": 10.0, "p95_ms": 20.0, "p99_ms": 30.0}
        if "perf:top_urls" in sql:
            return [row]
        if "perf:top_asns" in sql:
            return [{**row, "value": "7922"}]
        return []


def test_high_scale_wire_identical(monkeypatch):
    captured: dict = {}

    def _capture(cls, **kwargs):
        captured.update(kwargs)
        return cls(**kwargs)

    monkeypatch.setattr(PerformanceAggregatesResponse, "with_telemetry", classmethod(_capture))
    monkeypatch.setattr(
        hs_performance, "_aggregate", lambda *a, **k: SimpleNamespace(top_values=[("0", 3), ("45", 4), ("90000", 1)])
    )
    service = SimpleNamespace(service_id="svc", client=_HsClient())
    hs_performance.performance_aggregates(service, SimpleNamespace(), None, None)
    assert captured["top_asns"][0]["asn"] == "7922"
    assert_sections_wire_identical(
        PerformanceAggregatesResponse, captured, non_empty=("top_urls", "top_asns", "ttl_dist")
    )
