"""TDD regression tests for Group B Cloud vs High-Scale parity fixes."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.rum_ingest import _parse_rum_line
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import dashboard as hs_dashboard
from backend.high_scale import performance as hs_performance
from backend.high_scale import rum as hs_rum
from backend.high_scale.archive import HighScaleBatch, ServingWatermark
from backend.high_scale.archive.clickhouse_batch import _rows_for_domain
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.query import _normalize_serving_fields
from backend.models.analytics import AggregatesRequest, CmcdRequest, FieldValuesRequest, RawRequest
from backend.models.common import FilterSpec

pytestmark = pytest.mark.no_postgres


class _PredicateClient:
    """In-memory SQLite-free recording ClickHouse fake returning configurable rows."""

    def __init__(self, handler=None) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self._handler = handler

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params) if params is not None else None))
        if self._handler is not None:
            return self._handler(sql, params or {})
        return []


def _service(client: Any, *, start: datetime | None = None, end: datetime | None = None) -> HighScaleService:
    s = start or datetime(2026, 10, 23, 16, 0, 0, tzinfo=UTC)
    e = end or datetime(2026, 10, 23, 18, 0, 0, tzinfo=UTC)
    return HighScaleService(
        service_id="svc-parity-test",
        client=client,
        cursor_secret=b"parity-secret",
        request_watermark=ServingWatermark("svc-parity-test", "request", 1, s, e, None, None, None, True),
    )


def test_dashboard_half_open_window_excludes_exact_end_everywhere() -> None:
    start = datetime(2026, 10, 23, 16, 0, 0, tzinfo=UTC)
    end = datetime(2026, 10, 23, 18, 0, 0, tzinfo=UTC)

    def _handle(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "FROM request_aggregates" in sql and "chart_bucket" in sql:
            return [{"chart_bucket": "2026-10-23T16:00:00Z", "value": 5}]
        if "FROM request_aggregates" in sql and "GROUP BY dimension, value" in sql:
            return [
                {"dimension": "url", "value": "/a", "aggregate_count": 5},
                {"dimension": "country", "value": "US", "aggregate_count": 5},
            ]
        if "FROM request_aggregates" in sql and "GROUP BY dimension" in sql:
            return [{"dimension": "url", "total_count": 5}, {"dimension": "country", "total_count": 5}]
        if "FROM request_aggregates" in sql:
            return [{"value": "/a", "aggregate_count": 5, "total_count": 5}]
        return []

    client = _PredicateClient(_handle)
    svc = _service(client, start=start, end=end)
    res = hs_dashboard.dashboard_aggregates(
        svc,
        AggregatesRequest(
            fields=["url"], sections=["core", "topten"], chart_metric="requests", chart_interval="1 minute"
        ),
        start.isoformat(),
        end.isoformat(),
    )
    assert res.total_rows == 5
    # Exact-end fact query (event_timestamp = {end_exact}) must not run or inflate counts.
    assert not any("end_exact" in (p or {}) for _, p in client.calls)

    # Filtered path & metric != requests must use strict `< {end_upper}` (half-open [start, end)).
    client.calls.clear()
    hs_dashboard.dashboard_aggregates(
        svc,
        AggregatesRequest(
            fields=["url"],
            filters={"status": FilterSpec(mode="include", values=["200"])},
            chart_metric="p95_latency",
            chart_interval="1 minute",
        ),
        start.isoformat(),
        end.isoformat(),
    )
    fact_calls = [(sql, p) for sql, p in client.calls if "FROM request_facts" in sql]
    assert fact_calls
    for sql, _ in fact_calls:
        assert "event_timestamp < {end_upper:DateTime64(3)}" in sql
        assert "event_timestamp <= {end_upper:DateTime64(3)}" not in sql

    client.calls.clear()
    hs_dashboard.field_values(svc, FieldValuesRequest(field="url", search="a"), start.isoformat(), end.isoformat())
    hs_dashboard.raw_df(svc, RawRequest(), start.isoformat(), end.isoformat())
    for sql, _ in client.calls:
        if "event_timestamp" in sql:
            assert "event_timestamp < {end_upper:DateTime64(3)}" in sql
            assert "event_timestamp <= {end_upper:DateTime64(3)}" not in sql


@pytest.mark.parametrize(
    ("metric", "expected_tokens"),
    [
        ("p50_latency", ("quantile", "0.5", "elapsed")),
        ("p95_latency", ("quantile", "0.95", "elapsed")),
        ("p99_latency", ("quantile", "0.99", "elapsed")),
        ("ttfb", ("quantile", "0.95", "ottfb", "ttfb")),
        ("ttfb_ms", ("quantile", "0.5", "ttfb")),
        ("req_size", ("quantile", "0.5", "req_bytes", "req_header_bytes")),
        ("throughput", ("resp_bytes", "HIT")),
        ("bandwidth", ("resp_bytes",)),
        ("error_rate", ("400", "599")),
    ],
)
def test_dashboard_facts_time_series_implements_all_chart_metrics(
    metric: str, expected_tokens: tuple[str, ...]
) -> None:
    client = _PredicateClient(
        lambda sql, p: [{"bucket_start": "2026-10-23T16:00:00Z", "value": 12.5, "_bucket_cnt": 3}]
    )
    svc = _service(client)
    pts = hs_dashboard._facts_time_series(
        svc,
        "2026-10-23T16:00:00Z",
        "2026-10-23T18:00:00Z",
        "1 minute",
        metric,
        "service_id={service_id:String}",
        {"service_id": "svc-parity-test"},
    )
    assert pts and pts[0].value == 12.5
    sql, _ = client.calls[0]
    for tok in expected_tokens:
        assert tok in sql, f"metric={metric} missing {tok!r} in SQL: {sql}"
    assert "AS value" in sql

    client.calls.clear()
    pts2, cnt = hs_dashboard._facts_time_series_with_count(
        svc,
        "2026-10-23T16:00:00Z",
        "2026-10-23T18:00:00Z",
        "1 minute",
        metric,
        "service_id={service_id:String}",
        {"service_id": "svc-parity-test"},
    )
    assert cnt == 3 and pts2[0].value == 12.5
    sql2, _ = client.calls[0]
    for tok in expected_tokens:
        assert tok in sql2


def test_dashboard_map_data_returns_more_than_ten_countries() -> None:
    countries = [(f"C{i:02d}", 100 - i) for i in range(15)]

    def _handle(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "FROM request_facts" in sql and "field_name" in sql:
            out = [{"field_name": "url", "v": "/a", "c": 500}]
            out.extend({"field_name": "country", "v": c, "c": cnt} for c, cnt in countries)
            return out
        if "FROM request_facts" in sql and "GROUP BY v" in sql:
            if "country AS v" in sql or "country" in sql and "url AS v" not in sql:
                return [{"v": c, "c": cnt} for c, cnt in countries]
            return [{"v": "/a", "c": 500}]
        if "FROM request_facts" in sql and "count() AS c" in sql:
            return [{"c": 500}]
        if "FROM request_facts" in sql:
            return [{"bucket_start": "2026-10-23T16:00:00Z", "value": 500, "_bucket_cnt": 500}]
        return []

    client = _PredicateClient(_handle)
    svc = _service(client)
    res = hs_dashboard.dashboard_aggregates(
        svc,
        AggregatesRequest(
            fields=["url", "country"],
            filters={"status": FilterSpec(mode="include", values=["200"])},
            sections=["core", "topten"],
            chart_metric="requests",
        ),
        "2026-10-23T16:00:00Z",
        "2026-10-23T18:00:00Z",
    )
    assert len(res.map_data) == 15
    assert len(res.data["country"].top) == 10


def test_performance_ttl_dist_uses_canonical_four_buckets() -> None:
    seen_sql: list[str] = []

    def _handle(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        seen_sql.append(sql)
        return [{"bucket": "< 60s", "count": 5, "min_ttl": -1.0}, {"bucket": "1m - 1h", "count": 7, "min_ttl": 60.0}]

    svc = _service(_PredicateClient(_handle))
    res = hs_performance.performance_aggregates(
        svc,
        SimpleNamespace(filters={}, bucket_seconds=300, sort_by="p99"),
        "2026-10-23T12:00:00Z",
        "2026-10-23T18:00:00Z",
        sections={"ttl_dist"},
    )
    ttl_sql = seen_sql[0]
    for label in ("'< 60s'", "'1m - 1h'", "'1h - 1d'", "'> 1d'"):
        assert label in ttl_sql
    assert "< 60" in ttl_sql and "< 3600" in ttl_sql and "< 86400" in ttl_sql
    assert "<10s" not in ttl_sql and "<30m" not in ttl_sql
    assert [(b.bucket, b.count) for b in res.ttl_dist] == [("< 60s", 5), ("1m - 1h", 7)]


def test_cmcd_half_open_window_and_direct_request_filters() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def _handle(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        if "cmcd:probe" in sql:
            return [
                {
                    "events": 5,
                    "sessions": 2,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_sf": 1,
                    "has_su": 0,
                    "has_tb": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "rebuffer_sessions": 0,
                    "avg_br_v": 1000.0,
                    "avg_br_all": 1000.0,
                    "avg_bl_v": 500.0,
                    "avg_bl_all": 500.0,
                    "med_mtp_v": 2000.0,
                    "med_mtp_all": 2000.0,
                }
            ]
        return []

    svc = _service(_PredicateClient(_handle))
    res = hs_cmcd.cmcd_aggregates(
        svc,
        CmcdRequest(bucket_seconds=300, filters={"status": FilterSpec(mode="include", values=["200"])}),
        "2026-10-23T12:00:00Z",
        "2026-10-23T18:00:00Z",
        {"overview", "sessions_ts"},
        False,
    )
    assert res["has_data"] is True
    # 6h at 300s step with [start, end) half-open bounds is 72 buckets, not 73.
    assert len(res["sessions_ts"]) == 72
    probe_sql = calls[0][0]
    assert "event_timestamp < {end:DateTime64(3)}" in probe_sql
    assert "event_timestamp <= {end:DateTime64(3)}" not in probe_sql
    # Request-level filters query request_facts directly without broken request_event_id subquery.
    assert "FROM request_facts" in probe_sql
    assert "request_event_id IN (SELECT event_id" not in probe_sql


def test_clickhouse_batch_uses_matching_event_uuid_namespace_for_request_links() -> None:
    ts = "2026-10-23T16:00:00Z"
    req_batch = HighScaleBatch(
        "b-1",
        "svc-parity-test",
        "request",
        "1",
        (
            {
                "event_id": "raw-evt-1",
                "timestamp": ts,
                "source_object_key": "k",
                "source_object_version": "v",
                "line_ordinal": 0,
                "transform_version": "1",
                "country": "US",
                "client_ip": "1.2.3.4",
                "url": "/v",
                "cmcd": {"sid": "s1"},
            },
        ),
    )
    cmcd_batch = HighScaleBatch(
        "b-2",
        "svc-parity-test",
        "cmcd",
        "1",
        ({"request_event_id": "raw-evt-1", "timestamp": ts, "fields": {"sid": "s1"}, "country": "US"},),
    )
    req_uuid = _rows_for_domain(req_batch, "batch-uuid-1")[0][1]
    cmcd_req_uuid = _rows_for_domain(cmcd_batch, "batch-uuid-2")[0][2]
    assert req_uuid == cmcd_req_uuid


def test_rum_half_open_bounds_and_percent_decoding() -> None:
    calls: list[str] = []

    def _handle(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append(sql)
        if "rum_analytics_counts" in sql:
            return [{"beacons": 1, "pageviews": 1, "interactions": 0, "errors": 0}]
        return []

    svc = _service(_PredicateClient(_handle))
    res = hs_rum.rum_analytics(svc, "2026-10-23T16:00:00Z", "2026-10-23T18:00:00Z")
    # 2h at 1h buckets in [16:00, 18:00) -> 2 buckets (16:00, 17:00), not 3.
    assert res["trends"]["timestamps"] == ["2026-10-23T16:00:00+00:00", "2026-10-23T17:00:00+00:00"]
    for sql in calls:
        assert "event_timestamp < {end:DateTime64(3, 'UTC')}" in sql
        assert "event_timestamp <= {end:DateTime64(3, 'UTC')}" not in sql

    vitals, errors = _parse_rum_line(
        {
            "timestamp": "2026-10-23T16:00:05Z",
            "rum_pathname": "%2Frum-demo%2F",
            "rum_error_message": "Uncaught%20RangeError%3A%20boom",
            "rum_error_file": "https%3A%2F%2Fexample.test%2Fapp.js",
            "rum_cid": "cid-1",
        },
        "svc-parity-test",
    )
    assert not vitals and len(errors) == 1
    assert errors[0]["pathname"] == "/rum-demo/"
    assert errors[0]["error_message"] == "Uncaught RangeError: boom"
    assert errors[0]["error_file"] == "https://example.test/app.js"

    ev = {
        "url": "/rum-beacon?rum_pathname=%2",
        "rum_pathname": "%2Frum-demo%2F",
        "rum_error_message": "ReferenceError%3A%20x",
    }
    _normalize_serving_fields(ev, "rum_errors")
    assert ev["pathname"] == "/rum-demo/"
    assert ev["error_message"] == "ReferenceError: x"
