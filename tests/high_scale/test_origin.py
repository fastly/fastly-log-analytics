from types import SimpleNamespace

import pytest

from backend.high_scale.origin import aggregates


class _Client:
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
            return [
                {
                    "value": "/slow",
                    "requests": 5,
                    "p50_us": 1000.0,
                    "p95_us": 2500.0,
                    "p99_us": 3000.0,
                }
            ]
        if "origin:status_codes" in sql:
            return [{"value": "503", "requests": 2, "total_requests": 10}]
        if "origin:path_breakdown" in sql:
            return [{"value": "false", "requests": 4, "p50_us": 1200.0, "p95_us": 2400.0}]
        if "origin:pop_latency" in sql:
            return [
                {"value": "SJC", "requests": 4, "p50_us": 1000.0, "p95_us": 1000.0},
                {"value": "IAD", "requests": 3, "p50_us": 1200.0, "p95_us": 2000.0},
                {"value": "LAX", "requests": 3, "p50_us": 1500.0, "p95_us": 5000.0},
            ]
        if "origin:ip_health" in sql:
            return [
                {
                    "value": "203.0.113.1",
                    "requests": 7,
                    "origin_5xx": 1,
                    "p50_us": 1000.0,
                    "p95_us": 3000.0,
                }
            ]
        raise AssertionError(sql)


def _request(**overrides):
    values = {
        "filters": {},
        "bucket_minutes": 5,
        "split_by_leg": False,
        "timeseries_metric": "ttfb",
        "timeseries_percentile": "p95",
        "slow_urls_limit": 20,
        "slow_urls_min_requests": 1,
        "ip_health_limit": 30,
        "pop_latency_limit": 30,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_shapes_all_origin_sections_from_bounded_projection_queries() -> None:
    client = _Client()
    service = SimpleNamespace(service_id="svc", client=client)

    result = aggregates(
        service,
        _request(),
        "2026-09-14T10:00:00Z",
        "2026-09-14T11:00:00Z",
        sections={
            "summary",
            "timeseries",
            "slow_urls",
            "status_codes",
            "path_breakdown",
            "pop_latency",
            "ip_health",
        },
    )

    assert result["has_data"] is True
    assert result["summary"]["ottfb_p95_ms"] == 2.5
    assert result["summary"]["origin_error_rate"] == 0.1
    assert result["timeseries"]["series"][0]["value"] == 2.5
    assert result["slow_urls"]["rows"][0]["url"] == "/slow"
    assert result["status_codes"]["rows"][0] == {"status": 503, "count": 2, "pct": 20.0}
    assert result["path_breakdown"]["shielding_detected"] is True
    assert result["pop_latency"]["rows"][2]["elevated"] is True
    assert result["ip_health"]["rows"][0]["error_pct"] == pytest.approx(14.285714)
    assert all("high_scale_batch_publications FINAL" in sql for sql, _ in client.calls)
    assert all(params["service_id"] == "svc" for _, params in client.calls)
    assert client.calls[2][1]["limit"] == 20
    dimension_queries = [sql for sql, _ in client.calls if "FROM origin_minute_dimensions" in sql]
    assert dimension_queries
    assert all("sum(origin_minute_dimensions.requests)" in sql for sql in dimension_queries)
    assert all("sum(requests)" not in sql for sql in dimension_queries)
    assert "HAVING sum(origin_minute_dimensions.requests) >= 10" in client.calls[-1][0]


def test_rejects_filters_that_cannot_be_preserved_by_origin_projections() -> None:
    service = SimpleNamespace(service_id="svc", client=_Client())

    with pytest.raises(ValueError, match="filters"):
        aggregates(service, _request(filters={"country": ["US"]}), None, None, sections={"summary"})


def test_only_queries_requested_sections() -> None:
    client = _Client()
    service = SimpleNamespace(service_id="svc", client=client)

    result = aggregates(service, _request(), None, None, sections={"summary"})

    assert len(client.calls) == 1
    assert "summary" in result
    assert "timeseries" not in result


def test_rejects_subminute_timeseries_buckets() -> None:
    service = SimpleNamespace(service_id="svc", client=_Client())

    with pytest.raises(ValueError, match="bucket_minutes"):
        aggregates(
            service,
            _request(bucket_minutes=0.5),
            None,
            None,
            sections={"timeseries"},
        )
