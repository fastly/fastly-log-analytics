from dataclasses import replace
from datetime import UTC, datetime

import pytest

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.dashboard import aggregates, bundle, header_metrics
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import AggregatesRequest


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
            return [
                {
                    "total_rows": {"request": 12, "rum_vitals": 8, "rum_errors": 2}[domain],
                    "earliest_log_at": {
                        "request": datetime(2026, 9, 15, 18, 0, tzinfo=UTC),
                        "rum_vitals": datetime(2026, 9, 15, 18, 1, tzinfo=UTC),
                        "rum_errors": datetime(2026, 9, 15, 18, 2, tzinfo=UTC),
                    }[domain],
                    "latest_log_at": {
                        "request": datetime(2026, 9, 15, 19, 0, tzinfo=UTC),
                        "rum_vitals": datetime(2026, 9, 15, 19, 1, tzinfo=UTC),
                        "rum_errors": datetime(2026, 9, 15, 19, 2, tzinfo=UTC),
                    }[domain],
                }
            ]
        return [
            {"value": "/synthetic", "aggregate_count": 12, "total_count": 12},
        ]


def test_high_scale_dashboard_uses_clickhouse_aggregate_shape():
    service = HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )
    response = aggregates(
        service,
        AggregatesRequest(fields=["url", "country"]),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )
    assert response.total_rows == 12
    assert response.data["url"].total == 12
    assert response.map_data
    assert response.time_series[0].value == 12
    assert response.time_series[0].time == "2026-09-15T19:00:00Z"


def test_high_scale_dashboard_filtered_aggregates_returns_utc_iso_timestamps():
    service = HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )
    response = aggregates(
        service,
        AggregatesRequest(
            filters={"host": {"mode": "include", "values": ["example.com"]}},
            chart_interval="1 minute",
            chart_metric="requests",
        ),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )
    assert response.time_series[0].value == 12
    assert response.time_series[0].time == "2026-09-15T19:00:00Z"


def test_high_scale_dashboard_returns_requested_supported_card_dimensions():
    service = HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )

    response = aggregates(
        service,
        AggregatesRequest(fields=["host", "method", "status", "cache", "ua"]),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )

    assert set(response.data) == {"host", "method", "status", "cache", "ua"}
    assert all(field.total == 12 for field in response.data.values())


def test_high_scale_header_metrics_use_visible_rows_and_latest_events():
    service = HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )

    metrics = header_metrics(service)

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
    assert metrics["earliest_log_at"] == "2026-09-15T18:00:00+00:00"
    assert metrics["latest_log_at"] == "2026-09-15T19:00:00+00:00"
    assert metrics["local_rows"] == 22


def test_high_scale_dashboard_reuses_request_watermark_once_per_bundle():
    calls = {"count": 0}

    def request_watermark():
        calls["count"] += 1
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

    service = HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=request_watermark,
    )

    response = aggregates(
        service,
        AggregatesRequest(fields=["url", "country", "host"]),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )

    assert response.total_rows == 12
    assert calls["count"] == 1


def _service_with_watermark() -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )


def test_high_scale_aggregates_surfaces_watermark_extents():
    response = aggregates(
        _service_with_watermark(),
        AggregatesRequest(fields=["url", "country"]),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )
    assert response.earliest_log_at == "2026-09-15T00:00:00+00:00"
    assert response.latest_log_at == "2026-09-16T00:00:00+00:00"


def test_high_scale_filtered_aggregates_surfaces_watermark_extents():
    response = aggregates(
        _service_with_watermark(),
        AggregatesRequest(
            filters={"host": {"mode": "include", "values": ["example.com"]}},
            chart_interval="1 minute",
            chart_metric="requests",
        ),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )
    assert response.earliest_log_at == "2026-09-15T00:00:00+00:00"
    assert response.latest_log_at == "2026-09-16T00:00:00+00:00"


def test_high_scale_time_series_queries_through_in_flight_minute():
    executed_sql: list[str] = []

    class CapturingClient(FakeClient):
        def execute(self, sql, params=None):
            executed_sql.append(sql)
            return super().execute(sql, params)

    service = HighScaleService(
        service_id="svc",
        client=CapturingClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )

    aggregates(
        service,
        AggregatesRequest(fields=["url"]),
        "2026-09-15T00:00:00Z",
        "2026-09-15T19:00:00Z",
    )

    ts_queries = [s for s in executed_sql if "FROM request_aggregates" in s]
    assert ts_queries, "expected request_aggregates query"
    assert any("bucket_start <= {end:DateTime64(3)}" in q for q in ts_queries)


@pytest.mark.parametrize(
    ("interval", "seconds"),
    [("1 minute", 60), ("5 minutes", 300), ("1 hour", 3600), ("1 day", 86400)],
)
def test_high_scale_time_series_groups_by_returned_interval(interval: str, seconds: int) -> None:
    executed: list[tuple[str, dict]] = []

    class CapturingClient(FakeClient):
        def execute(self, sql, params=None):
            executed.append((sql, params or {}))
            return super().execute(sql, params)

    service = replace(_service_with_watermark(), client=CapturingClient())
    response = aggregates(
        service,
        AggregatesRequest(fields=["url"], chart_interval=interval),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )

    query, params = next(
        (sql, params) for sql, params in executed if "FROM request_aggregates" in sql and "AS value" in sql
    )
    assert "toStartOfInterval(bucket_start, INTERVAL {bucket_seconds:UInt32} SECOND)" in query
    assert "GROUP BY chart_bucket ORDER BY chart_bucket" in query
    assert params["bucket_seconds"] == seconds
    assert response.interval == interval
    assert response.time_series[0].value == 12


def test_high_scale_one_second_chart_uses_request_facts() -> None:
    executed: list[str] = []

    class CapturingClient(FakeClient):
        def execute(self, sql, params=None):
            executed.append(sql)
            return super().execute(sql, params)

    service = replace(_service_with_watermark(), client=CapturingClient())
    response = aggregates(
        service,
        AggregatesRequest(fields=["url"], chart_interval="1 second"),
        "2026-09-15T00:00:00Z",
        "2026-09-16T00:00:00Z",
    )

    assert any("toStartOfSecond(event_timestamp)" in sql for sql in executed)
    assert not any("FROM request_aggregates" in sql for sql in executed)
    assert response.interval == "1 second"


class _RecordingClient(FakeClient):
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params or {}))
        return super().execute(sql, params)


@pytest.mark.parametrize("filtered", [False, True])
def test_dashboard_attributes_projection_and_fact_dispatch_and_reuses_historical_cache(monkeypatch, filtered):
    from backend.core.high_scale_sections import current_clickhouse_scope

    client = _RecordingClient()
    service = replace(_service_with_watermark(), client=client)
    scopes = []
    execute = client.execute

    def observed_execute(sql, params=None):
        scopes.append(current_clickhouse_scope())
        return execute(sql, params)

    monkeypatch.setattr(client, "execute", observed_execute)
    req = AggregatesRequest(
        fields=["url"], filters={"host": {"mode": "include", "values": ["example.com"]}} if filtered else {}
    )
    response = aggregates(service, req, *_WINDOW)
    assert scopes
    assert len(scopes) == len(client.calls)
    assert all(
        scope["service_id"] == "svc" and scope["source"] == ("fact" if filtered else "projection") for scope in scopes
    )
    assert all(scope["section_id"] in {row["section_id"] for row in response.section_timings} for scope in scopes)
    calls_before = len(client.calls)
    hit = aggregates(service, req, *_WINDOW)
    assert hit.response_cache.state == "hit"
    assert len(client.calls) == calls_before
    assert hit.section_timings == response.section_timings


def _recording_service() -> tuple[HighScaleService, _RecordingClient]:
    client = _RecordingClient()
    return replace(_service_with_watermark(), client=client), client


_WINDOW = ("2026-09-15T00:00:00Z", "2026-09-16T00:00:00Z")


def test_filtered_aggregates_use_the_shared_filter_translation_and_visibility_fence():
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
        assert "country IN ({flt_0_0:String})" in sql
        assert "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
        assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
        assert params["flt_0_0"] == "US" and params["flt_1_0"] == "SJC"


def test_filters_on_fields_outside_the_card_list_are_still_applied():
    service, client = _recording_service()

    aggregates(
        service,
        AggregatesRequest(fields=["url"], filters={"rid": {"mode": "include", "values": ["abc"]}}),
        *_WINDOW,
    )

    assert any(params.get("flt_k0") == "rid" for _, params in client.calls)


def test_filters_the_fact_table_cannot_express_fail_loudly():
    service, _ = _recording_service()

    with pytest.raises(ValueError, match="do not support filtering"):
        aggregates(
            service,
            AggregatesRequest(filters={"_bot_name": {"mode": "include", "values": ["googlebot"]}}),
            *_WINDOW,
        )


@pytest.mark.parametrize("failing", ["AS v, count() AS c", "count() AS c FROM request_facts", "AS bucket_start"])
def test_filtered_aggregate_failures_propagate_instead_of_rendering_empty_cards(failing):
    class Failing(FakeClient):
        def execute(self, sql, params=None):
            if failing in sql:
                raise RuntimeError("clickhouse unavailable")
            return super().execute(sql, params)

    service = replace(_service_with_watermark(), client=Failing())

    with pytest.raises(RuntimeError, match="unavailable"):
        aggregates(
            service,
            AggregatesRequest(fields=["url"], filters={"host": {"mode": "include", "values": ["example.com"]}}),
            *_WINDOW,
        )


def test_include_time_series_false_skips_the_time_series_query():
    service, client = _recording_service()

    response = aggregates(service, AggregatesRequest(fields=["url"], include_time_series=False), *_WINDOW)

    assert response.time_series == []
    assert all("AS chart_bucket" not in sql for sql, _ in client.calls)


def test_include_map_data_false_skips_the_map_and_never_adds_country_work():
    service, client = _recording_service()

    response = aggregates(
        service, AggregatesRequest(fields=["url"], include_map_data=False, include_time_series=False), *_WINDOW
    )

    assert response.map_data == []
    assert not any(params.get("dimension") == "country" for _, params in client.calls)


def test_map_data_is_served_even_when_country_is_not_a_requested_card():
    service, _ = _recording_service()

    response = aggregates(service, AggregatesRequest(fields=["url"]), *_WINDOW)

    assert response.map_data
    assert "country" not in response.data


def test_sections_selector_gates_the_panels_like_the_standard_router():
    service, client = _recording_service()

    topten = aggregates(service, AggregatesRequest(fields=["url"], sections=["topten"]), *_WINDOW)

    assert topten.data and topten.time_series == [] and topten.map_data == []

    core = aggregates(service, AggregatesRequest(fields=["url"], sections=["core"]), *_WINDOW)

    assert core.data == {} and core.time_series and core.map_data


def test_bundle_serves_top_bots_from_the_security_reader(monkeypatch):
    from backend.high_scale import security as hs_security
    from backend.models.security import SecurityTopBotsResponse

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
    service, _ = _recording_service()

    result = bundle(service, AggregatesRequest(fields=["url"]), *_WINDOW)

    assert seen == [("svc", *_WINDOW)]
    assert result["top_bots"].ngwaf_bots[0].id == "googlebot"
    assert result["aggregates"].data


def test_bundle_sections_skip_the_unrequested_branch(monkeypatch):
    from backend.high_scale import security as hs_security

    monkeypatch.setattr(hs_security, "top_bots", lambda *a, **k: pytest.fail("bots were not requested"))
    service, client = _recording_service()

    core_only = bundle(service, AggregatesRequest(fields=["url"], sections=["core"]), *_WINDOW)

    assert core_only["top_bots"] is None and core_only["aggregates"] is not None

    from backend.models.security import SecurityTopBotsResponse

    monkeypatch.setattr(
        hs_security, "top_bots", lambda *a, **k: SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=[])
    )
    client.calls.clear()
    bots_only = bundle(service, AggregatesRequest(sections=["bots"]), *_WINDOW)

    assert bots_only["aggregates"] is None and bots_only["top_bots"] is not None
    assert client.calls == []


def test_header_metrics_surface_clickhouse_failures_instead_of_reporting_no_data():
    class _Failing:
        def execute(self, sql, params=None):
            if "rum_error_aggregates" in sql:
                raise RuntimeError("clickhouse down")
            return []

    service = HighScaleService(service_id="svc", client=_Failing(), cursor_secret=b"s", request_watermark=None)

    with pytest.raises(RuntimeError, match="clickhouse down"):
        header_metrics(service)
