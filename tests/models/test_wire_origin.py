"""Origin response row/section models keep every producer's wire bytes."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.core.duckdb import _clear_schema_cache
from backend.high_scale.origin import aggregates as hs_aggregates
from backend.models.origin import (
    OriginAggregatesResponse,
    OriginDrilldownResponse,
    OriginIpHealthResponse,
    OriginPathBreakdownResponse,
    OriginPopLatencyResponse,
    OriginShieldingAnalysisResponse,
    OriginSlowUrlsResponse,
    OriginStatusCodesResponse,
    OriginSummaryResponse,
    OriginTimeseriesResponse,
)
from backend.repositories import origin as origin_mod
from backend.repositories._base import _safe_table
from tests._helpers.wire import assert_sections_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


@pytest.fixture(autouse=True)
def _clear_origin_caches():
    _clear_schema_cache()
    origin_mod._response_cache.clear()
    yield
    _clear_schema_cache()
    origin_mod._response_cache.clear()


def _seed(con, src, n=60):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log["ottfb"] = 50000 + i * 1000
        log["ottlb"] = 90000 + i * 1000
        log["elapsed"] = log["ottlb"] + 5000
        log["ost"] = 200 if i % 5 else 500
        if i == 3:
            log["ost"] = 829
        log["oip"] = "203.0.113.1" if i < 25 else "203.0.113.2"
        log["url"] = "/api/data" if i % 2 == 0 else "/images/logo.png"
        log["pop"] = ["SJC", "IAD", "LAX"][i % 3]
        log["host"] = "example.com" if i % 2 else "www.example.com"
        log["edge"] = i % 4 != 0
        log["cache"] = "MISS" if i % 3 == 0 else ("PASS" if i % 3 == 1 else "HIT")
        log["rid"] = f"rid-{i}"
        log["prid"] = f"rid-{i - 1}" if i % 4 == 0 and i else None
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def _call(fn, con, src, **kw):
    return fn(con=con, src=src, start_time=None, end_time=None, filters={}, **kw)


def test_granular_repositories_wire_identical(in_memory_duckdb, test_service_source):
    _seed(in_memory_duckdb, test_service_source)
    con, src = in_memory_duckdb, test_service_source
    assert_sections_wire_identical(
        OriginSummaryResponse, _call(origin_mod.get_summary, con, src), non_empty=("has_data",)
    )
    assert_sections_wire_identical(
        OriginTimeseriesResponse, _call(origin_mod.get_timeseries, con, src), non_empty=("series",)
    )
    split = _call(origin_mod.get_timeseries, con, src, split_by_leg=True)
    assert any("edge" in point for point in split["series"])
    assert_sections_wire_identical(OriginTimeseriesResponse, split, non_empty=("series",))
    assert_sections_wire_identical(
        OriginSlowUrlsResponse, _call(origin_mod.get_slow_urls, con, src, min_requests=1), non_empty=("rows",)
    )
    assert_sections_wire_identical(
        OriginStatusCodesResponse, _call(origin_mod.get_status_codes, con, src), non_empty=("rows",)
    )
    assert_sections_wire_identical(
        OriginPathBreakdownResponse, _call(origin_mod.get_path_breakdown, con, src), non_empty=("rows",)
    )
    assert_sections_wire_identical(
        OriginPopLatencyResponse, _call(origin_mod.get_pop_latency, con, src), non_empty=("rows",)
    )
    assert_sections_wire_identical(
        OriginIpHealthResponse, _call(origin_mod.get_ip_health, con, src), non_empty=("rows",)
    )
    for dim in ("host", "pop", "oip"):
        assert_sections_wire_identical(
            OriginDrilldownResponse, _call(origin_mod.get_drilldown, con, src, dimension=dim), non_empty=("rows",)
        )
    assert_sections_wire_identical(
        OriginShieldingAnalysisResponse, _call(origin_mod.get_shielding_analysis, con, src), non_empty=("rows",)
    )


@pytest.mark.asyncio
async def test_composite_aggregates_wire_identical(in_memory_duckdb, test_service_source):
    _seed(in_memory_duckdb, test_service_source)
    res = await origin_mod.get_aggregates(
        in_memory_duckdb, test_service_source, None, None, {}, slow_urls_min_requests=1
    )
    assert_sections_wire_identical(
        OriginAggregatesResponse,
        res,
        non_empty=("summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"),
    )
    for section in ("timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"):
        assert res[section]["rows" if section != "timeseries" else "series"], section


@pytest.mark.asyncio
async def test_composite_aggregates_empty_schema_wire_identical(in_memory_duckdb):
    src = {"name": "origin_wire_empty", "service_id": "origin_wire_empty"}
    res = await origin_mod.get_aggregates(in_memory_duckdb, src, None, None, {})
    assert_sections_wire_identical(OriginAggregatesResponse, res)


class _HsClient:
    def execute(self, sql, params=None):
        if "origin:summary" in sql:
            return [
                {
                    "requests": 10,
                    "misses": 6,
                    "passes": 2,
                    "origin_5xx": 1,
                    "status_count": 10,
                    "latency_p50_us": 1000.0,
                    "latency_p75_us": 1500.0,
                    "latency_p95_us": 2500.0,
                    "latency_p99_us": 3000.0,
                    "ttlb_p50_us": 2000.0,
                    "ttlb_p95_us": 4000.0,
                    "cdn_overhead_p50_us": 500.0,
                    "origin_bytes_p50": 128.0,
                }
            ]
        if "origin:timeseries" in sql:
            return [{"time": "2026-09-14 10:00:00", "miss_count": 6, "value_us": 2500.0}]
        if "origin:slow_urls" in sql:
            return [{"value": "/slow", "requests": 5, "p50_us": 1000.0, "p95_us": 2500.0, "p99_us": 3000.0}]
        if "origin:status_codes" in sql:
            return [{"value": "503", "requests": 2, "total_requests": 10}]
        if "origin:path_breakdown" in sql:
            return [
                {"value": "false", "requests": 4, "p50_us": 1200.0, "p95_us": 2400.0},
                {"value": "true", "requests": 6, "p50_us": 1000.0, "p95_us": 2000.0},
            ]
        if "origin:pop_latency" in sql:
            return [
                {"value": "SJC", "requests": 4, "p50_us": 1000.0, "p95_us": 1000.0},
                {"value": "LAX", "requests": 3, "p50_us": 1500.0, "p95_us": 5000.0},
            ]
        if "origin:ip_health" in sql:
            return [{"value": "203.0.113.1", "requests": 7, "origin_5xx": 1, "p50_us": 1000.0, "p95_us": 2000.0}]
        return []


def test_high_scale_aggregates_wire_identical():
    service = SimpleNamespace(service_id="svc", client=_HsClient())
    req = SimpleNamespace(
        filters={},
        split_by_leg=False,
        bucket_minutes=5,
        timeseries_metric="ttfb",
        timeseries_percentile="p95",
        slow_urls_limit=20,
        slow_urls_min_requests=1,
        pop_latency_limit=30,
        ip_health_limit=30,
    )
    res = hs_aggregates(service, req, None, None, sections=None)
    assert_sections_wire_identical(
        OriginAggregatesResponse,
        res,
        non_empty=("summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"),
    )


def test_high_scale_empty_sections_wire_identical():
    class _Empty:
        def execute(self, sql, params=None):
            return []

    service = SimpleNamespace(service_id="svc", client=_Empty())
    req = SimpleNamespace(
        filters={},
        split_by_leg=False,
        bucket_minutes=5,
        timeseries_metric="ttfb",
        timeseries_percentile="p95",
        slow_urls_limit=20,
        slow_urls_min_requests=1,
        pop_latency_limit=30,
        ip_health_limit=30,
    )
    res = hs_aggregates(service, req, None, None, sections=None)
    assert_sections_wire_identical(OriginAggregatesResponse, res)
