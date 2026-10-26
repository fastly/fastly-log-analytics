from __future__ import annotations

import json
import re
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.insights import cache_collapse_detail_endpoint, insights_endpoint
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.models.dashboard import AggregatesRequest
from backend.models.security import SecurityTopBotsResponse

from backend.high_scale import insights as hs_insights
from backend.high_scale import security as hs_security
from backend.high_scale.dashboard import aggregates, bundle, header_metrics
from backend.high_scale.insight_queries import COLUMNS, DETECTIONS, botnet_grouping
from backend.high_scale.origin import aggregates as origin_aggregates
from backend.models.common import FilterSpec
from backend.models.insights import CacheCollapseDetailRequest, InsightsRequest
from backend.repositories.insights import registry

_WINDOW = ("2026-09-15T00:00:00Z", "2026-09-16T00:00:00Z")


class FakeClient:
    def execute(self, sql, params=None):
        if "AS chart_bucket" in sql:
            return [{"chart_bucket": "2026-09-15 19:00:00", "value": 12}]
        if "AS bucket_start" in sql or "SELECT bucket_start" in sql:
            return [{"bucket_start": "2026-09-15 19:00:00", "value": 12}]
        if "AS v, count() AS c" in sql:
            return [{"v": "/synthetic", "c": 12}]
        if "count() AS c FROM request_facts" in sql:
            return [{"c": 12}]
        if "max(event_timestamp)" in sql:
            domain = (params or {}).get("domain")
            rows_map = {"request": (12, 0), "rum_vitals": (8, 1), "rum_errors": (2, 2)}
            total, min_off = rows_map[domain]
            return [
                {
                    "total_rows": total,
                    "earliest_log_at": datetime(2026, 9, 15, 18, min_off, tzinfo=UTC),
                    "latest_log_at": datetime(2026, 9, 15, 19, min_off, tzinfo=UTC),
                }
            ]
        return [{"value": "/synthetic", "aggregate_count": 12, "total_count": 12}]


class _RecordingClient(FakeClient):
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params or {}))
        return super().execute(sql, params)


def _default_watermark() -> ServingWatermark:
    return ServingWatermark(
        service_id="svc",
        domain="request",
        owner_epoch=1,
        coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=None,
        exact=True,
    )


def _service_with_watermark(client: Any = None, watermark: Any = None) -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=client or FakeClient(),
        cursor_secret=b"secret",
        request_watermark=_default_watermark() if watermark is None else watermark,
    )


def _recording_service() -> tuple[HighScaleService, _RecordingClient]:
    client = _RecordingClient()
    return _service_with_watermark(client=client), client


# ── Dashboard Aggregates, Header Metrics & Bundle ────────────────────────────


def test_high_scale_dashboard_aggregates_and_extents():
    service = _service_with_watermark()
    response = aggregates(service, AggregatesRequest(fields=["url", "country"]), *_WINDOW)
    assert response.total_rows == 12 and response.data["url"].total == 12 and response.map_data
    assert response.time_series[0].value == 12 and response.time_series[0].time == "2026-09-15T19:00:00Z"
    assert response.earliest_log_at == "2026-09-15T00:00:00+00:00"
    assert response.latest_log_at == "2026-09-16T00:00:00+00:00"

    flt_resp = aggregates(
        service,
        AggregatesRequest(
            filters={"host": {"mode": "include", "values": ["example.com"]}},
            chart_interval="1 minute",
            chart_metric="requests",
        ),
        *_WINDOW,
    )
    assert flt_resp.time_series[0].value == 12 and flt_resp.time_series[0].time == "2026-09-15T19:00:00Z"
    assert flt_resp.earliest_log_at == "2026-09-15T00:00:00+00:00"
    assert flt_resp.latest_log_at == "2026-09-16T00:00:00+00:00"

    dims_resp = aggregates(service, AggregatesRequest(fields=["host", "method", "status", "cache", "ua"]), *_WINDOW)
    assert set(dims_resp.data) == {"host", "method", "status", "cache", "ua"}
    assert all(field.total == 12 for field in dims_resp.data.values())


def test_high_scale_header_metrics_and_clickhouse_failure_propagation():
    metrics = header_metrics(_service_with_watermark())
    assert metrics["request"] == {
        "earliest_log_at": "2026-09-15T18:00:00+00:00",
        "latest_log_at": "2026-09-15T19:00:00+00:00",
        "total_rows": 12,
        "last_sync_at": None,
    }
    assert metrics["rum"] == {
        "earliest_log_at": "2026-09-15T18:01:00+00:00",
        "latest_log_at": "2026-09-15T19:02:00+00:00",
        "total_rows": 10,
        "last_sync_at": None,
    }
    assert (
        metrics["earliest_log_at"] == "2026-09-15T18:00:00+00:00"
        and metrics["latest_log_at"] == "2026-09-15T19:00:00+00:00"
    )
    assert metrics["local_rows"] == 22

    class _Failing:
        def execute(self, sql, params=None):
            if "rum_error_aggregates" in sql:
                raise RuntimeError("clickhouse down")
            return []

    with pytest.raises(RuntimeError, match="clickhouse down"):
        header_metrics(
            HighScaleService(service_id="svc", client=_Failing(), cursor_secret=b"s", request_watermark=None)
        )


def test_high_scale_dashboard_reuses_request_watermark_once_per_bundle():
    calls = {"count": 0}

    def request_watermark():
        calls["count"] += 1
        return _default_watermark()

    service = _service_with_watermark(watermark=request_watermark)
    response = aggregates(service, AggregatesRequest(fields=["url", "country", "host"]), *_WINDOW)
    assert response.total_rows == 12 and calls["count"] == 1


def test_high_scale_time_series_intervals_and_end_minute_fence():
    service, client = _recording_service()
    aggregates(service, AggregatesRequest(fields=["url"]), "2026-09-15T00:00:00Z", "2026-09-15T19:00:00Z")
    ts_queries = [s for s, _ in client.calls if "FROM request_aggregates" in s]
    assert ts_queries and any("bucket_start < {end:DateTime64(3)}" in q for q in ts_queries)

    client.calls.clear()
    resp_1s = aggregates(service, AggregatesRequest(fields=["url"], chart_interval="1 second"), *_WINDOW)
    assert any("toStartOfSecond(event_timestamp)" in s for s, _ in client.calls)
    assert not any("FROM request_aggregates" in s for s, _ in client.calls)
    assert resp_1s.interval == "1 second"


@pytest.mark.parametrize(
    ("interval", "seconds"), [("1 minute", 60), ("5 minutes", 300), ("1 hour", 3600), ("1 day", 86400)]
)
def test_high_scale_time_series_groups_by_returned_interval(interval: str, seconds: int) -> None:
    service, client = _recording_service()
    response = aggregates(service, AggregatesRequest(fields=["url"], chart_interval=interval), *_WINDOW)
    query, params = next((sql, p) for sql, p in client.calls if "FROM request_aggregates" in sql and "AS value" in sql)
    assert "toStartOfInterval(bucket_start, INTERVAL {bucket_seconds:UInt32} SECOND)" in query
    assert "GROUP BY chart_bucket ORDER BY chart_bucket" in query
    assert params["bucket_seconds"] == seconds and response.interval == interval and response.time_series[0].value == 12


@pytest.mark.parametrize("filtered", [False, True])
def test_dashboard_attributes_projection_and_fact_dispatch_and_reuses_historical_cache(monkeypatch, filtered):
    from backend.core.high_scale_sections import current_clickhouse_scope

    service, client = _recording_service()
    scopes = []
    execute = client.execute
    monkeypatch.setattr(
        client, "execute", lambda sql, params=None: (scopes.append(current_clickhouse_scope()), execute(sql, params))[1]
    )
    req = AggregatesRequest(
        fields=["url"], filters={"host": {"mode": "include", "values": ["example.com"]}} if filtered else {}
    )
    response = aggregates(service, req, *_WINDOW)
    assert scopes and len(scopes) == len(client.calls)
    assert all(s["service_id"] == "svc" and s["source"] == ("fact" if filtered else "projection") for s in scopes)
    assert all(s["section_id"] in {row["section_id"] for row in response.section_timings} for s in scopes)

    calls_before = len(client.calls)
    hit = aggregates(service, req, *_WINDOW)
    assert hit.response_cache.state == "hit" and len(client.calls) == calls_before
    assert hit.section_timings == response.section_timings


@pytest.mark.parametrize(
    ("start_time", "end_time"),
    [
        ("2026-09-15T00:00:17Z", "2026-09-16T00:00:00Z"),
        ("2026-09-15T00:00:00Z", "2026-09-16T00:00:17Z"),
        ("2026-09-15T00:00:00.001Z", "2026-09-16T00:00:00Z"),
        ("2026-09-15T00:00:00Z", "2026-09-16T00:00:00.001Z"),
    ],
)
def test_unfiltered_subminute_windows_use_scoped_facts(start_time: str, end_time: str) -> None:
    service, client = _recording_service()
    response = aggregates(service, AggregatesRequest(fields=["url", "country"]), start_time, end_time)
    start, end = datetime.fromisoformat(start_time), datetime.fromisoformat(end_time)
    assert client.calls
    for sql, params in client.calls:
        assert "FROM request_facts" in sql and "high_scale_batch_publications FINAL" in sql
        assert (
            "event_timestamp >= {start:DateTime64(3)}" in sql and "event_timestamp < {end_upper:DateTime64(3)}" in sql
        )
        assert params["service_id"] == "svc" and params["start"] == start and params["end_upper"] == end
    assert all(row["source"] == "fact" for row in response.section_timings if row["activity"] != "overhead")


def test_filtered_aggregates_translation_fence_and_unsupported_fields():
    service, client = _recording_service()
    aggregates(
        service,
        AggregatesRequest(
            fields=["url"],
            filters={"country": {"mode": "include", "values": ["US"]}, "pop": {"mode": "exclude", "values": ["SJC"]}},
        ),
        *_WINDOW,
    )
    facts = [(sql, params) for sql, params in client.calls if "FROM request_facts" in sql]
    assert facts
    for sql, params in facts:
        assert (
            "country IN ({flt_0_0:String})" in sql and "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
        )
        assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
        assert params["flt_0_0"] == "US" and params["flt_1_0"] == "SJC"

    client.calls.clear()
    aggregates(
        service, AggregatesRequest(fields=["url"], filters={"rid": {"mode": "include", "values": ["abc"]}}), *_WINDOW
    )
    assert any(params.get("flt_k0") == "rid" for _, params in client.calls)

    with pytest.raises(ValueError, match="do not support filtering"):
        aggregates(
            service, AggregatesRequest(filters={"_bot_name": {"mode": "include", "values": ["googlebot"]}}), *_WINDOW
        )


@pytest.mark.parametrize("failing", ["AS v, count() AS c", "count() AS c FROM request_facts", "AS bucket_start"])
def test_filtered_aggregate_failures_propagate_instead_of_rendering_empty_cards(failing):
    class Failing(FakeClient):
        def execute(self, sql, params=None):
            if failing in sql:
                raise RuntimeError("clickhouse unavailable")
            return super().execute(sql, params)

    service = _service_with_watermark(client=Failing())
    with pytest.raises(RuntimeError, match="unavailable"):
        aggregates(
            service,
            AggregatesRequest(fields=["url"], filters={"host": {"mode": "include", "values": ["example.com"]}}),
            *_WINDOW,
        )


def test_panel_flags_and_sections_selector():
    service, client = _recording_service()
    r_no_ts = aggregates(service, AggregatesRequest(fields=["url"], include_time_series=False), *_WINDOW)
    assert r_no_ts.time_series == [] and all("AS chart_bucket" not in sql for sql, _ in client.calls)

    client.calls.clear()
    r_no_map = aggregates(
        service, AggregatesRequest(fields=["url"], include_map_data=False, include_time_series=False), *_WINDOW
    )
    assert r_no_map.map_data == [] and not any(p.get("dimension") == "country" for _, p in client.calls)

    r_map_default = aggregates(service, AggregatesRequest(fields=["url"]), *_WINDOW)
    assert r_map_default.map_data and "country" not in r_map_default.data

    topten = aggregates(service, AggregatesRequest(fields=["url"], sections=["topten"]), *_WINDOW)
    assert topten.data and topten.time_series == [] and topten.map_data == []

    core = aggregates(service, AggregatesRequest(fields=["url"], sections=["core"]), *_WINDOW)
    assert core.data == {} and core.time_series and core.map_data


def test_bundle_top_bots_and_section_gating(monkeypatch):
    seen: list[tuple] = []

    def fake_top_bots(service, req, start_time, end_time):
        seen.append((service.service_id, start_time, end_time))
        return SecurityTopBotsResponse.with_telemetry(
            bots=[],
            ngwaf_bots=[
                {
                    "id": "googlebot",
                    "name": "Googlebot",
                    "category": "search",
                    "request_count": 5,
                    "verified_count": 5,
                    "impersonator_count": 0,
                    "unverified_count": 0,
                    "verification_coverage": 1.0,
                    "pending_count": 0,
                }
            ],
        )

    monkeypatch.setattr(hs_security, "top_bots", fake_top_bots)
    service, client = _recording_service()
    result = bundle(service, AggregatesRequest(fields=["url"]), *_WINDOW)
    assert (
        seen == [("svc", *_WINDOW)] and result["top_bots"].ngwaf_bots[0].id == "googlebot" and result["aggregates"].data
    )

    monkeypatch.setattr(hs_security, "top_bots", lambda *a, **k: pytest.fail("bots were not requested"))
    core_only = bundle(service, AggregatesRequest(fields=["url"], sections=["core"]), *_WINDOW)
    assert core_only["top_bots"] is None and core_only["aggregates"] is not None

    monkeypatch.setattr(
        hs_security, "top_bots", lambda *a, **k: SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=[])
    )
    client.calls.clear()
    bots_only = bundle(service, AggregatesRequest(sections=["bots"]), *_WINDOW)
    assert bots_only["aggregates"] is None and bots_only["top_bots"] is not None and client.calls == []


# ── Origin Aggregates & Projections ──────────────────────────────────────────


class _OriginClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "origin:summary" in sql:
            return [
                {
                    "requests": 10,
                    "misses": 6,
                    "passes": 2,
                    "origin_5xx": 1,
                    "status_count": 10,
                    "latency_q": [1000.0, 1500.0, 2500.0, 3000.0],
                    "latency_n": 10,
                    "ttlb_q": [2000.0, 4000.0],
                    "ttlb_n": 10,
                    "overhead_q": [500.0],
                    "overhead_n": 10,
                    "bytes_q": [128.0],
                    "bytes_n": 10,
                }
            ]
        if "origin:timeseries" in sql:
            return [{"time": "2026-09-14 10:00:00", "miss_count": 6, "value_q": [2500.0], "value_n": 6}]
        if "origin:slow_urls" in sql:
            return [{"value": "/slow", "requests": 5, "latency_q": [1000.0, 2500.0, 3000.0], "latency_n": 5}]
        if "origin:status_codes" in sql:
            return [{"value": "503", "requests": 2, "total_requests": 10}]
        if "origin:path_breakdown" in sql:
            return [{"value": "false", "requests": 4, "latency_q": [1200.0, 2400.0], "latency_n": 4}]
        if "origin:pop_latency" in sql:
            return [
                {"value": "SJC", "requests": 4, "latency_q": [1000.0, 1000.0], "latency_n": 4},
                {"value": "IAD", "requests": 3, "latency_q": [1200.0, 2000.0], "latency_n": 3},
                {"value": "LAX", "requests": 3, "latency_q": [1500.0, 5000.0], "latency_n": 3},
            ]
        if "origin:ip_health" in sql:
            return [
                {"value": "203.0.113.1", "requests": 7, "origin_5xx": 1, "latency_q": [1000.0, 3000.0], "latency_n": 7}
            ]
        raise AssertionError(sql)


class _OriginFactsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "origin:facts_summary" in sql:
            return [
                {
                    "requests": 10,
                    "misses": 6,
                    "passes": 2,
                    "origin_5xx": 1,
                    "status_count": 10,
                    "latency_q": [1000.0, 1500.0, 2500.0, 3000.0],
                    "ttlb_n": 10,
                    "ttlb_q": [2000.0, 4000.0],
                    "overhead_n": 10,
                    "overhead_q": 500.0,
                    "bytes_n": 10,
                    "bytes_q": 128.0,
                }
            ]
        if "origin:facts_slow_urls" in sql:
            return [{"value": "/slow", "requests": 5, "latency_q": [1000.0, 2500.0, 3000.0]}]
        if "origin:facts_status_codes" in sql:
            return [{"value": 503, "requests": 2}, {"value": 200, "requests": 8}]
        if "origin:facts_pop_latency" in sql:
            return [{"value": "SJC", "requests": 4, "origin_5xx": 0, "latency_n": 4, "latency_q": [1000.0, 2000.0]}]
        raise AssertionError(sql)


def _origin_request(**overrides):
    return SimpleNamespace(
        **{
            "filters": {},
            "bucket_minutes": 5,
            "split_by_leg": False,
            "timeseries_metric": "ttfb",
            "timeseries_percentile": "p95",
            "slow_urls_limit": 20,
            "slow_urls_min_requests": 1,
            "ip_health_limit": 30,
            "pop_latency_limit": 30,
            **overrides,
        }
    )


_ALL_ORIGIN_SECTIONS = {
    "summary",
    "timeseries",
    "slow_urls",
    "status_codes",
    "path_breakdown",
    "pop_latency",
    "ip_health",
}


def test_shapes_all_origin_sections_from_bounded_projection_queries() -> None:
    client = _OriginClient()
    service = SimpleNamespace(service_id="svc", client=client)
    result = origin_aggregates(
        service, _origin_request(), "2026-09-14T10:00:00Z", "2026-09-14T11:00:00Z", sections=_ALL_ORIGIN_SECTIONS
    )

    assert result["has_data"] is True
    assert result["summary"]["ottfb_p95_ms"] == 2.5 and result["summary"]["origin_error_rate"] == 0.1
    assert result["timeseries"]["series"][0]["value"] == 2.5 and result["slow_urls"]["rows"][0]["url"] == "/slow"
    assert result["status_codes"]["rows"][0] == {"status": 503, "count": 2, "pct": 20.0}
    assert (
        result["path_breakdown"]["shielding_detected"] is True and result["pop_latency"]["rows"][2]["elevated"] is True
    )
    assert result["ip_health"]["rows"][0]["error_pct"] == pytest.approx(14.285714)
    assert all(
        "high_scale_batch_publications FINAL" in sql and params["service_id"] == "svc" for sql, params in client.calls
    )
    assert next(p for s, p in client.calls if "origin:slow_urls" in s)["limit"] == 20
    dim_queries = [sql for sql, _ in client.calls if "FROM origin_minute_dimensions" in sql]
    assert dim_queries and all(
        "sum(origin_minute_dimensions.requests)" in s and "sum(requests)" not in s for s in dim_queries
    )
    assert "HAVING sum(origin_minute_dimensions.requests) >= 10" in next(
        s for s, _ in client.calls if "origin:ip_health" in s
    )

    for section in ("summary", "timeseries", "slow_urls", "path_breakdown", "pop_latency", "ip_health"):
        assert result[section]["_approx"] is True
        assert "histogram sketch" in result[section]["_approx_note"] and "1%" in result[section]["_approx_note"]
    assert "_approx" not in result["status_codes"]

    percentile_queries = [sql for sql, _ in client.calls if "_q" in sql or "quantiles" in sql]
    assert percentile_queries
    for sql in percentile_queries:
        assert "quantilesExactWeightedArray" in sql and "_sk_v" in sql and "_sk_w" in sql
        assert "latency_p50_us" not in sql and "latency_p95_us" not in sql and "* latency_count" not in sql


def test_filtered_requests_read_request_facts_exactly_instead_of_failing() -> None:
    client = _OriginFactsClient()
    service = SimpleNamespace(service_id="svc", client=client)
    req = _origin_request(filters={"country": FilterSpec(mode="include", values=["US"])})
    result = origin_aggregates(
        service, req, None, None, sections={"summary", "slow_urls", "status_codes", "pop_latency"}
    )

    sqls = [sql for sql, _ in client.calls]
    assert sqls and all(
        "FROM (SELECT" in sql and "request_facts" in sql and "origin_minute_" not in sql for sql in sqls
    )
    assert all(params["filter_0_0"] == "US" for _, params in client.calls)
    assert result["summary"]["_approx"] is False and result["slow_urls"]["_approx"] is False
    assert result["summary"]["ottfb_p95_ms"] == 2.5 and result["summary"]["origin_error_rate"] == 0.1
    assert [row["status"] for row in result["status_codes"]["rows"]] == [503, 200]
    assert result["status_codes"]["rows"][0]["pct"] == 20.0


def test_origin_filter_validation_section_gating_and_bucket_guards() -> None:
    client = _OriginClient()
    service = SimpleNamespace(service_id="svc", client=client)
    origin_aggregates(
        service,
        _origin_request(filters={"country": FilterSpec(mode="include", values=[])}),
        None,
        None,
        sections={"summary"},
    )
    assert "origin_minute_summary" in client.calls[0][0]

    client.calls.clear()
    res = origin_aggregates(service, _origin_request(), None, None, sections={"summary"})
    assert len(client.calls) == 1 and "summary" in res and "timeseries" not in res

    with pytest.raises(ValueError, match="_bot_name"):
        origin_aggregates(
            service,
            _origin_request(filters={"_bot_name": FilterSpec(mode="include", values=["x"])}),
            None,
            None,
            sections={"summary"},
        )

    with pytest.raises(ValueError, match="bucket_minutes"):
        origin_aggregates(service, _origin_request(bucket_minutes=0.5), None, None, sections={"timeseries"})


def test_origin_empty_sketch_observations_handling() -> None:
    class _NoSketch(_OriginClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "origin:slow_urls" in sql:
                return [{"value": "/legacy", "requests": 9, "latency_q": [0.0, 0.0, 0.0], "latency_n": 0}]
            if "origin:summary" in sql:
                return [
                    {
                        "requests": 4,
                        "misses": 0,
                        "passes": 0,
                        "origin_5xx": 0,
                        "status_count": 4,
                        "latency_q": [0.0] * 4,
                        "latency_n": 0,
                        "ttlb_q": [0.0, 0.0],
                        "ttlb_n": 0,
                        "overhead_q": [0.0],
                        "overhead_n": 0,
                        "bytes_q": [0.0],
                        "bytes_n": 0,
                    }
                ]
            return super().execute(sql, params)

    service = SimpleNamespace(service_id="svc", client=_NoSketch())
    res_slow = origin_aggregates(service, _origin_request(), None, None, sections={"slow_urls"})
    row = res_slow["slow_urls"]["rows"][0]
    assert row["requests"] == 9 and row["p50_ms"] is None and row["p95_ms"] is None and row["p99_ms"] is None

    res_sum = origin_aggregates(service, _origin_request(), None, None, sections={"summary"})
    assert res_sum["has_data"] is False and res_sum["summary"]["has_data"] is False


def test_builds_minute_summary_dimension_rows_and_mergeable_sketches():
    events = (
        {
            "timestamp": "2026-09-16T14:00:01Z",
            "cache": "MISS",
            "ost": 200,
            "ottfb": 1000,
            "ottlb": 1800,
            "elapsed": 2300,
            "obytes": 100,
            "url": "/a",
            "pop": "SJC",
            "oip": "192.0.2.10",
            "edge": True,
        },
        {
            "timestamp": "2026-09-16T14:00:40Z",
            "cache": "PASS",
            "ost": 503,
            "ttfb": 0.002,
            "ottlb": 2800,
            "elapsed": 3400,
            "obytes": 300,
            "url": "/a",
            "pop": "SJC",
            "oip": "192.0.2.10",
            "edge": False,
        },
        {
            "timestamp": "2026-09-16T14:01:00Z",
            "cache": "HIT",
            "ost": 829,
            "ottfb": "",
            "url": "/b",
            "pop": "IAD",
            "oip": "192.0.2.20",
            "edge": True,
        },
    )
    original = deepcopy(events)
    projection = build_origin_projection_rows(events)
    assert events == original
    (summary_row,) = projection.summary_rows
    for metric in ("latency", "ttlb", "overhead", "origin_bytes"):
        assert sum(summary_row[f"{metric}_sk_w"]) == 2
        assert len(summary_row[f"{metric}_sk_v"]) == len(summary_row[f"{metric}_sk_w"])
    for key in [k for k in summary_row if "_sk_" in k]:
        del summary_row[key]
    assert projection.summary_rows == (
        {
            "bucket_start": "2026-09-16T14:00:00+00:00",
            "requests": 2,
            "misses": 1,
            "passes": 1,
            "origin_5xx": 1,
            "status_count": 2,
            "origin_bytes": 400,
            "latency_count": 2,
            "ttlb_count": 2,
            "overhead_count": 2,
            "origin_bytes_count": 2,
            "latency_p50_us": 1500.0,
            "latency_p75_us": 1750.0,
            "latency_p95_us": 1950.0,
            "latency_p99_us": 1990.0,
            "ttlb_p50_us": 2300.0,
            "ttlb_p95_us": 2750.0,
            "cdn_overhead_p50_us": 550.0,
            "origin_bytes_p50": 200.0,
        },
    )
    url = next(row for row in projection.dimension_rows if row["dimension"] == "url" and row["value"] == "/a")
    assert url["requests"] == 2 and url["origin_5xx"] == 1 and url["latency_p50_us"] == 1500.0
    status = next(row for row in projection.dimension_rows if row["dimension"] == "status" and row["value"] == "-1")
    assert (
        status["requests"] == 1
        and status["latency_count"] == 0
        and status["latency_sk_v"] == []
        and status["latency_sk_w"] == []
    )

    # Identical observations merge into a single sketch bin of weight 2
    same_proj = build_origin_projection_rows((events[0], {**events[0], "timestamp": "2026-09-16T14:00:40Z"}))
    (same_sum,) = same_proj.summary_rows
    assert same_sum["latency_sk_w"] == [2] and same_sum["latency_sk_v"][0] == pytest.approx(1000.0, rel=0.01)

    # Invalid timestamps and NaN values are skipped
    bad_proj = build_origin_projection_rows(
        (
            {"timestamp": "invalid", "ottfb": 10},
            {"timestamp": "2026-09-16T14:00:00Z", "ottfb": float("nan"), "ost": "bad"},
        )
    )
    assert bad_proj.summary_rows == () and bad_proj.dimension_rows == ()


# ── Insights & Cache Collapse Detail ─────────────────────────────────────────

FINGERPRINT_DETECTIONS = {"botnet_grouping", "repeated_patterns_fp", "impossible_distance"}
EVERY_CARD = set(DETECTIONS) | FINGERPRINT_DETECTIONS
ALL_FIELDS = sorted({name for detection in DETECTIONS.values() for name in detection.columns} - {"signal"}) + [
    "waf_sig",
    "ja4",
    "oconnect_ms",
    "edge",
    "lat",
    "lon",
]


@pytest.fixture(autouse=True)
def _pop_coordinates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}
    )


class _InsightsClient:
    def __init__(
        self,
        rows: dict[str, list[dict[str, Any]]] | None = None,
        *,
        history_hours: float = 400.0,
        fields: list[str] | None = None,
        fail: set[str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._rows = rows or {}
        self._history_hours = history_hours
        self._fields = ALL_FIELDS if fields is None else fields
        self._fail = fail or set()

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        self.calls.append((sql, params))
        marker = re.search(r"/\* insight:(\w+) \*/", sql)
        assert marker, sql
        name = marker.group(1)
        if name in self._fail:
            raise RuntimeError("clickhouse unavailable")
        if name == "history":
            return [{"earliest": params["window_end"] - timedelta(hours=self._history_hours), "window_total": 1234}]
        if name == "fields":
            return [{"fields": self._fields}]
        return self._rows.get(name, [])

    def queries(self) -> dict[str, tuple[str, dict]]:
        return {re.search(r"/\* insight:(\w+) \*/", sql).group(1): (sql, params) for sql, params in self.calls}  # type: ignore[union-attr]


def _run_insights(
    client: _InsightsClient, *, mask_ips: bool = False, window: float = 24.0, baseline: float = 168.0, **clamp: Any
):
    service = SimpleNamespace(service_id="svc", client=client)
    req = InsightsRequest(window_size_hrs=window, baseline_hours=baseline)
    return insights_endpoint(service, req, clamp.get("clamp_start"), clamp.get("clamp_end"), mask_ips)  # type: ignore[arg-type]


def _insight_cards(response) -> dict[str, dict]:
    return {card["id"]: card for card in json.loads(response.model_dump_json(by_alias=True))["insights"]}


def test_detections_registry_and_investigate_cards() -> None:
    assert callable(hs_insights.insights_endpoint) and callable(hs_insights.cache_collapse_detail_endpoint)
    registered = {definition.id: definition for definition in registry.get_all()}
    for insight_id, detection in DETECTIONS.items():
        assert insight_id in registered and registered[insight_id].row_processor is not None
        assert set(detection.columns) <= set(COLUMNS), insight_id
    assert botnet_grouping("ja3").columns == ("ip", "ja3")
    with pytest.raises(ValueError, match="ja4 or ja3"):
        botnet_grouping("x; DROP TABLE y")

    client = _InsightsClient(
        {
            "error_spikes": [
                {"url": "/api", "w_rate": 0.62, "b_rate": 0.01, "w_errors": 31, "w_total": 50, "b_total": 400},
                {"url": "/b", "w_rate": 0.10, "b_rate": 0.0, "w_errors": 5, "w_total": 50, "b_total": 400},
            ]
        }
    )
    cards = _insight_cards(_run_insights(client))
    card = cards["error_spikes"]
    assert (
        card["severity"] == "critical"
        and card["summary"] == "2 URLs with elevated server error rates"
        and card["category"] == "origin"
    )
    first = card["items"][0]
    assert first["label"] == "/api" and first["unit"] == "% 5xx"
    assert first["current_val"] == pytest.approx(62.0) and first["baseline_val"] == pytest.approx(1.0)
    assert first["meta"]["errors"] == 31 and first["meta"]["filters"] == {"url": "/api"}
    assert (
        first["investigate_url"].startswith("/dashboard?service=svc&start=")
        and "filter_url=%2Fapi" in first["investigate_url"]
    )
    assert (
        cards["latency_regression"]["severity"] == "clean"
        and cards["latency_regression"]["summary"] == "No latency regression detected"
    )


def test_every_detection_runs_and_query_is_visible_scoped_and_parameterized() -> None:
    client = _InsightsClient()
    response = _run_insights(client)
    cards = _insight_cards(response)
    assert set(cards) == EVERY_CARD == {definition.id for definition in registry.get_all()}
    assert all(card["severity"] == "clean" for card in cards.values())
    assert json.loads(response.model_dump_json(by_alias=True))["window_hours"] == 24.0

    queries = client.queries()
    for insight_id in EVERY_CARD:
        sql, params = queries[insight_id]
        assert (
            "FROM request_facts" in sql
            and "service_id={service_id:String}" in sql
            and "publication_state='visible'" in sql
        )
        assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
        assert "event_timestamp <= {window_end:DateTime64(3)}" in sql
        window_only = (insight_id in DETECTIONS and DETECTIONS[insight_id].window_only) or insight_id in {
            "repeated_patterns_fp",
            "impossible_distance",
        }
        lower = "{window_start:DateTime64(3)}" if window_only else "{baseline_start:DateTime64(3)}"
        if insight_id != "botnet_grouping":
            assert f"event_timestamp >= {lower}" in sql, insight_id
        assert (
            params["service_id"] == "svc" and params["baseline_start"] < params["window_start"] < params["window_end"]
        )

    for sql, params in client.calls:
        assert "svc" not in sql
        assert re.findall(r"\{(\w+):", sql) and all(name in params for name in re.findall(r"\{(\w+):", sql))


def test_insufficient_history_and_missing_fields_or_pop_coords(monkeypatch: pytest.MonkeyPatch) -> None:
    client_short = _InsightsClient(history_hours=5.0)
    cards_short = _insight_cards(_run_insights(client_short))
    assert set(cards_short) == EVERY_CARD
    for insight_id, card in cards_short.items():
        if insight_id == "impossible_distance":
            assert card["severity"] == "clean"
        else:
            assert (
                card["severity"] == "info"
                and card["summary"] == "Requires 168h of historical data (only 5.0h available)"
                and card["items"] == []
            )
    assert {re.search(r"insight:(\w+)", sql).group(1) for sql, _ in client_short.calls} == {
        "history",
        "fields",
        "impossible_distance",
    }  # type: ignore[union-attr]

    # Omitted latency fields
    client_no_lat = _InsightsClient(fields=[n for n in ALL_FIELDS if n not in {"elapsed", "ottfb"}])
    cards_no_lat = _insight_cards(_run_insights(client_no_lat))
    for omitted in ("latency_regression", "tail_latency", "pop_latency_regression", "origin_latency_spike"):
        assert omitted not in cards_no_lat and omitted not in client_no_lat.queries()
    assert "error_spikes" in cards_no_lat

    # Omitted TLS fingerprint vs JA3 fallback
    client_no_fp = _InsightsClient(fields=[n for n in ALL_FIELDS if n != "ja4"])
    assert not (FINGERPRINT_DETECTIONS & set(_insight_cards(_run_insights(client_no_fp))))
    assert "ja4 != ''" in client_no_lat.queries()["botnet_grouping"][0]
    client_ja3 = _InsightsClient(fields=[n for n in ALL_FIELDS if n != "ja4"] + ["ja3"])
    _run_insights(client_ja3)
    assert "ja3 != ''" in client_ja3.queries()["botnet_grouping"][0]

    # Impossible distance POP code sanitization and empty POP map
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map",
        lambda: {"SJC": (37.36, -121.93), "SJC'; DROP TABLE x": (1.0, 2.0)},
    )
    client_pop = _InsightsClient()
    _run_insights(client_pop)
    pop_sql = client_pop.queries()["impossible_distance"][0]
    assert "'SJC'" in pop_sql and "DROP TABLE" not in pop_sql

    monkeypatch.setattr("backend.utils.pop_utils.get_pop_lat_lon_map", dict)
    assert "impossible_distance" not in _insight_cards(_run_insights(_InsightsClient()))


def test_insights_failures_masking_asn_and_window_clamping(monkeypatch: pytest.MonkeyPatch) -> None:
    client_fail = _InsightsClient(
        {"error_spikes": [{"url": "/x", "w_rate": 0.9, "b_rate": 0.0, "w_errors": 9, "w_total": 10, "b_total": 99}]},
        fail={"latency_regression"},
    )
    cards = _insight_cards(_run_insights(client_fail))
    assert (
        cards["latency_regression"]["severity"] == "error"
        and cards["latency_regression"]["summary"] == "Query failed: RuntimeError"
    )
    assert cards["latency_regression"]["items"] == [] and cards["error_spikes"]["severity"] == "critical"

    with pytest.raises(RuntimeError, match="unavailable"):
        _run_insights(_InsightsClient(fail={"history"}))

    # Analyst IP masking
    rows = {
        "request_size_anomaly": [
            {"ip": "203.0.113.77", "max_bytes": 9000, "avg_bytes": 4000.0, "w_total": 8, "b_p95": 1000.0}
        ]
    }
    plain = _insight_cards(_run_insights(_InsightsClient(rows)))["request_size_anomaly"]["items"][0]
    masked = _insight_cards(_run_insights(_InsightsClient(rows), mask_ips=True))["request_size_anomaly"]["items"][0]
    assert "203.0.113.77" in json.dumps(plain) and "203.0.113.77" not in json.dumps(masked)

    # ASN ISP resolution
    seen: list[tuple[str, list]] = []
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names", lambda sid, asns: (seen.append((sid, list(asns))), {7922: "Comcast"})[1]
    )
    asn_rows = [{"asn": 7922, "w_cnt": 90, "b_cnt": 5, "b_total": 1000, "w_total": 120}]
    asn_card = _insight_cards(_run_insights(_InsightsClient({"asn_concentration": asn_rows})))["asn_concentration"]
    assert (
        seen == [("svc", [7922])]
        and asn_card["items"][0]["label"] == "AS7922 (Comcast)"
        and asn_card["severity"] == "critical"
    )

    # Clamp bounds & window parameters
    now = datetime.now(UTC)
    client_clamp = _InsightsClient()
    _run_insights(
        client_clamp,
        clamp_start=(now - timedelta(hours=30)).isoformat(),
        clamp_end=(now - timedelta(hours=2)).isoformat(),
    )
    _, h_params = client_clamp.queries()["history"]
    assert h_params["window_end"] <= now - timedelta(hours=2) + timedelta(seconds=1)
    assert h_params["baseline_start"] >= now - timedelta(hours=30) - timedelta(seconds=1)
    assert h_params["window_start"] >= h_params["baseline_start"]

    client_win = _InsightsClient()
    _run_insights(client_win, window=6.0, baseline=48.0)
    _, w_params = client_win.queries()["error_spikes"]
    assert w_params["window_hours"] == 6.0 and w_params["baseline_hours"] == 48.0
    assert w_params["window_end"] - w_params["window_start"] == pytest.approx(
        timedelta(hours=6), abs=timedelta(seconds=1)
    )
    assert w_params["window_start"] - w_params["baseline_start"] == pytest.approx(
        timedelta(hours=48), abs=timedelta(seconds=1)
    )


class _InsightsDetailClient:
    def __init__(self, *, empty: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._empty = empty

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "cache_collapse_rates" in sql:
            zeros = dict.fromkeys(("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total"), 0)
            return (
                [zeros]
                if self._empty
                else [
                    {
                        **zeros,
                        "w_hits": 10,
                        "w_miss": 30,
                        "w_pass": 10,
                        "w_total": 55,
                        "b_hits": 90,
                        "b_miss": 10,
                        "b_pass": 5,
                        "b_total": 120,
                    }
                ]
            )
        if "cache_collapse_timeline" in sql:
            return (
                []
                if self._empty
                else [
                    {"bucket": "2026-09-14 10:00:00", "total_requests": 40, "real_hits": 10, "misses": 30},
                    {"bucket": datetime(2026, 9, 14, 11, tzinfo=UTC), "total_requests": 4, "real_hits": 0, "misses": 0},
                ]
            )
        if "cache_collapse_misses" in sql:
            return (
                []
                if self._empty
                else [
                    {
                        "timestamp": "2026-09-14 10:59:00.000",
                        "cache": "MISS",
                        "pop": "SJC",
                        "ip": "203.0.113.9",
                        "status": 200,
                    },
                    {
                        "timestamp": datetime(2026, 9, 14, 10, 58, tzinfo=UTC),
                        "cache": "",
                        "pop": "",
                        "ip": "",
                        "status": None,
                    },
                ]
            )
        raise AssertionError(sql)


def _collapse_detail(client: Any, *, mask_ips: bool = False, window: float = 24.0, baseline: float = 168.0):
    req = CacheCollapseDetailRequest(url="/hot", window_size_hrs=window, baseline_hours=baseline)
    service = SimpleNamespace(service_id="svc", client=client)
    return json.loads(cache_collapse_detail_endpoint(service, req, None, None, mask_ips).model_dump_json(by_alias=True))  # type: ignore[arg-type]


def test_cache_collapse_detail_timeline_misses_scoping_and_empty() -> None:
    client = _InsightsDetailClient()
    result = _collapse_detail(client)
    assert result["url"] == "/hot" and result["has_data"] is True
    assert result["breakdown"] == {"hits": 10, "misses": 30, "passes": 10, "other": 5}
    assert result["window_hit_rate"] == pytest.approx(25.0) and result["baseline_hit_rate"] == pytest.approx(90.0)
    assert result["window_pass_rate"] == pytest.approx(10 / 55 * 100) and result["baseline_pass_rate"] == pytest.approx(
        5 / 120 * 100
    )
    first = result["timeline"][0]
    assert (
        first["bucket"] == "2026-09-14T10:00:00Z"
        and first["expected_hits"] == pytest.approx(40 * 0.9)
        and first["hit_rate"] == pytest.approx(0.25)
    )
    assert (
        result["timeline"][1]["hit_rate"] == 0.0
        and result["timeline"][1]["bucket"] == "2026-09-14T11:00:00Z"
        and result["clusters"] == []
    )

    plain = result["recent_misses"]
    masked = _collapse_detail(_InsightsDetailClient(), mask_ips=True)["recent_misses"]
    assert plain[0] == {
        "timestamp": "2026-09-14T10:59:00.000Z",
        "cache": "MISS",
        "pop": "SJC",
        "ip": "203.0.113.9",
        "status": 200,
    }
    assert plain[1]["cache"] == "UNKNOWN" and plain[1]["status"] is None and plain[1]["pop"] is None
    assert masked[0]["ip"] != "203.0.113.9"

    assert len(client.calls) == 3
    for sql, params in client.calls:
        assert "url = {url:String}" in sql and params["url"] == "/hot"
        assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
        assert "event_timestamp >= {baseline_start:DateTime64(3)}" in sql and params["service_id"] == "svc"

    empty_res = _collapse_detail(_InsightsDetailClient(empty=True))
    assert empty_res["has_data"] is False and empty_res["timeline"] == [] and empty_res["recent_misses"] == []
    assert empty_res["breakdown"] == {"hits": 0, "misses": 0, "passes": 0, "other": 0}


@pytest.mark.parametrize(
    ("window", "baseline", "interval"),
    [
        (1.0, 1.0, "INTERVAL 1 MINUTE"),
        (6.0, 6.0, "INTERVAL 5 MINUTE"),
        (24.0, 96.0, "INTERVAL 1 HOUR"),
        (24.0, 168.0, "INTERVAL 4 HOUR"),
    ],
)
def test_cache_collapse_detail_bucket_size_follows_the_lookback(window: float, baseline: float, interval: str) -> None:
    client = _InsightsDetailClient()
    _collapse_detail(client, window=window, baseline=baseline)
    assert interval in next(sql for sql, _ in client.calls if "cache_collapse_timeline" in sql)
