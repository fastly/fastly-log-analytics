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
            return [
                {
                    "value": "/slow",
                    "requests": 5,
                    "latency_q": [1000.0, 2500.0, 3000.0],
                    "latency_n": 5,
                }
            ]
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
                {
                    "value": "203.0.113.1",
                    "requests": 7,
                    "origin_5xx": 1,
                    "latency_q": [1000.0, 3000.0],
                    "latency_n": 7,
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
    slow_urls_call = next((sql, params) for sql, params in client.calls if "origin:slow_urls" in sql)
    assert slow_urls_call[1]["limit"] == 20
    dimension_queries = [sql for sql, _ in client.calls if "FROM origin_minute_dimensions" in sql]
    assert dimension_queries
    assert all("sum(origin_minute_dimensions.requests)" in sql for sql in dimension_queries)
    assert all("sum(requests)" not in sql for sql in dimension_queries)
    ip_health_sql = next(sql for sql, _ in client.calls if "origin:ip_health" in sql)
    assert "HAVING sum(origin_minute_dimensions.requests) >= 10" in ip_health_sql


class _FactsClient:
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


def test_filtered_requests_read_request_facts_exactly_instead_of_failing() -> None:
    from backend.models.common import FilterSpec

    client = _FactsClient()
    service = SimpleNamespace(service_id="svc", client=client)
    req = _request(filters={"country": FilterSpec(mode="include", values=["US"])})

    result = aggregates(service, req, None, None, sections={"summary", "slow_urls", "status_codes", "pop_latency"})

    sqls = [sql for sql, _ in client.calls]
    assert sqls and all("FROM (SELECT" in sql and "request_facts" in sql for sql in sqls)
    assert not any("origin_minute_" in sql for sql in sqls)
    assert all(params["filter_0_0"] == "US" for _, params in client.calls)
    assert result["summary"]["_approx"] is False
    assert result["slow_urls"]["_approx"] is False
    assert result["summary"]["ottfb_p95_ms"] == 2.5
    assert result["summary"]["origin_error_rate"] == 0.1
    assert [row["status"] for row in result["status_codes"]["rows"]] == [503, 200]
    assert result["status_codes"]["rows"][0]["pct"] == 20.0


def test_inactive_filters_still_use_the_projections() -> None:
    from backend.models.common import FilterSpec

    client = _Client()
    service = SimpleNamespace(service_id="svc", client=client)
    req = _request(filters={"country": FilterSpec(mode="include", values=[])})

    aggregates(service, req, None, None, sections={"summary"})

    assert "origin_minute_summary" in client.calls[0][0]


def test_unsupported_filter_fields_are_rejected_loudly_not_ignored() -> None:
    from backend.models.common import FilterSpec

    service = SimpleNamespace(service_id="svc", client=_Client())
    req = _request(filters={"_bot_name": FilterSpec(mode="include", values=["x"])})

    with pytest.raises(ValueError, match="_bot_name"):
        aggregates(service, req, None, None, sections={"summary"})


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


def test_percentile_sections_merge_sketches_exactly_instead_of_averaging_percentiles() -> None:
    client = _Client()
    service = SimpleNamespace(service_id="svc", client=client)

    aggregates(
        service,
        _request(),
        None,
        None,
        sections={"summary", "timeseries", "slow_urls", "path_breakdown", "pop_latency", "ip_health"},
    )

    percentile_queries = [sql for sql, _ in client.calls if "_q" in sql or "quantiles" in sql]
    assert percentile_queries
    for sql in percentile_queries:
        assert "quantilesExactWeightedArray" in sql
        assert "_sk_v" in sql and "_sk_w" in sql
        assert "latency_p50_us" not in sql and "latency_p95_us" not in sql
        assert "* latency_count" not in sql


def test_sketch_backed_sections_declare_how_they_are_approximate() -> None:
    service = SimpleNamespace(service_id="svc", client=_Client())

    result = aggregates(
        service,
        _request(),
        None,
        None,
        sections={"summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"},
    )

    for section in ("summary", "timeseries", "slow_urls", "path_breakdown", "pop_latency", "ip_health"):
        assert result[section]["_approx"] is True
        assert "histogram sketch" in result[section]["_approx_note"]
        assert "1%" in result[section]["_approx_note"]
    assert "_approx" not in result["status_codes"]


def test_percentiles_without_sketch_observations_are_none_not_zero() -> None:
    class _NoSketch(_Client):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "origin:slow_urls" in sql:
                return [{"value": "/legacy", "requests": 9, "latency_q": [0.0, 0.0, 0.0], "latency_n": 0}]
            return super().execute(sql, params)

    service = SimpleNamespace(service_id="svc", client=_NoSketch())

    result = aggregates(service, _request(), None, None, sections={"slow_urls"})

    row = result["slow_urls"]["rows"][0]
    assert row["requests"] == 9
    assert row["p50_ms"] is None and row["p95_ms"] is None and row["p99_ms"] is None


def test_summary_without_any_sketch_observations_reports_no_data() -> None:
    class _NoSketch(_Client):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "origin:summary" in sql:
                return [
                    {
                        "requests": 4,
                        "misses": 0,
                        "passes": 0,
                        "origin_5xx": 0,
                        "status_count": 4,
                        "latency_q": [0.0, 0.0, 0.0, 0.0],
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

    result = aggregates(service, _request(), None, None, sections={"summary"})

    assert result["has_data"] is False
    assert result["summary"]["has_data"] is False


# ── Merged from test_origin_projection.py ──

from copy import deepcopy

from backend.high_scale.origin_projection import build_origin_projection_rows


def test_builds_minute_summary_and_dimension_rows_without_mutating_events():
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
    for key in [key for key in summary_row if "_sk_" in key]:
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
    assert url["requests"] == 2
    assert url["origin_5xx"] == 1
    assert url["latency_p50_us"] == 1500.0
    status = next(row for row in projection.dimension_rows if row["dimension"] == "status" and row["value"] == "-1")
    assert status["requests"] == 1
    assert status["latency_count"] == 0


def test_skips_rows_without_parseable_timestamps_and_non_finite_numbers():
    projection = build_origin_projection_rows(
        (
            {"timestamp": "invalid", "ottfb": 10},
            {"timestamp": "2026-09-16T14:00:00Z", "ottfb": float("nan"), "ost": "bad"},
        )
    )

    assert projection.summary_rows == ()
    assert projection.dimension_rows == ()


def test_rows_carry_mergeable_sketches_for_every_percentile_metric():
    events = (
        {
            "timestamp": "2026-09-16T14:00:01Z",
            "ost": 200,
            "ottfb": 1000,
            "ottlb": 1800,
            "elapsed": 2300,
            "obytes": 100,
            "url": "/a",
        },
        {
            "timestamp": "2026-09-16T14:00:40Z",
            "ost": 200,
            "ottfb": 1000,
            "ottlb": 2800,
            "elapsed": 3400,
            "obytes": 300,
            "url": "/a",
        },
    )

    projection = build_origin_projection_rows(events)

    (summary,) = projection.summary_rows
    for metric, observations in (("latency", 2), ("ttlb", 2), ("overhead", 2), ("origin_bytes", 2)):
        assert sum(summary[f"{metric}_sk_w"]) == observations
        assert len(summary[f"{metric}_sk_v"]) == len(summary[f"{metric}_sk_w"])
    assert summary["latency_sk_w"] == [2]
    assert summary["latency_sk_v"][0] == pytest.approx(1000.0, rel=0.01)
    url = next(row for row in projection.dimension_rows if row["dimension"] == "url")
    assert url["latency_sk_w"] == [2]
    status = next(row for row in projection.dimension_rows if row["dimension"] == "status")
    assert status["latency_sk_v"] == [] and status["latency_sk_w"] == []
