"""Regression coverage for the native RUM chart producer."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.rum import rum_analytics


class ChartClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "rum_analytics_counts" in sql:
            return [{"beacons": 3, "pageviews": 1, "interactions": 1, "errors": 1}]
        if "rum_analytics_trends" in sql:
            return [
                {
                    "bucket": "2026-09-01 00:00:00",
                    "lcp": 2500.0,
                    "cls": 0.125,
                    "views": 2,
                    "pageviews": 1,
                    "interactions": 1,
                    "errors": 1,
                }
            ]
        if "GROUP BY metric_name" in sql:
            return [{"metric_name": "LCP", "p75": 2500.0, "total": 1, "good": 1, "ni": 0, "poor": 0}]
        # Let the old health/count code see data, so this regression specifically
        # fails on its hardcoded chart arrays rather than its onboarding branch.
        return [{"c": 1}] if " as c " in sql else []


def service_for(client: Any) -> HighScaleService:
    watermark = ServingWatermark("TestRum", "request", 0, None, None, None, None, None, True)
    return HighScaleService("TestRum", client, b"test-secret", watermark)


def test_real_rows_populate_aligned_chart_series() -> None:
    client = ChartClient()
    result = rum_analytics(service_for(client), "2026-09-01T00:15:00Z", "2026-09-01T02:15:00Z")
    trends = result["trends"]
    assert trends["timestamps"] == [
        "2026-09-01T00:00:00+00:00",
        "2026-09-01T01:00:00+00:00",
        "2026-09-01T02:00:00+00:00",
    ]
    assert trends["lcp"] == [2.5, None, None]
    assert trends["cls"] == [0.125, None, None]
    assert trends["error_rate"] == [33.33, None, None]
    assert trends["pageviews"] == [1, 0, 0]
    assert trends["interactions"] == [1, 0, 0]
    assert trends["errors"] == [1, 0, 0]
    assert result["vitals"]["lcp"]["p75"] == 2.5
    assert result["beacon_count"] == 3
    assert result["interaction_count"] == 1
    for sql, params in client.calls:
        assert "publication_state" in sql
        assert params["service_id"] == "TestRum"
        assert params["start"] == datetime(2026, 9, 1, 0, 15, tzinfo=UTC)
        assert params["end"] == datetime(2026, 9, 1, 2, 15, tzinfo=UTC)


@pytest.mark.parametrize("stage", ["counts", "vitals", "trends", "pages", "exceptions"])
def test_query_failure_is_not_successful_empty_data(stage: str) -> None:
    class BrokenClient(ChartClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if f"rum_analytics_{stage}" in sql:
                raise RuntimeError("query unavailable")
            return super().execute(sql, params)

    with pytest.raises(RuntimeError, match="query unavailable"):
        rum_analytics(service_for(BrokenClient()), "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z")


def test_no_data_probe_failure_propagates() -> None:
    class BrokenProbe(ChartClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "rum_analytics_counts" in sql:
                return [{"beacons": 0, "pageviews": 0, "interactions": 0, "errors": 0}]
            raise RuntimeError("probe unavailable")

    with pytest.raises(RuntimeError, match="probe unavailable"):
        rum_analytics(service_for(BrokenProbe()), "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z")


@pytest.mark.parametrize("start,end", [(None, None), ("2026-09-01T00:00:00", None), (None, "2026-09-02T00:00:00Z")])
def test_default_and_one_sided_bounds_are_always_applied(monkeypatch, start: str | None, end: str | None) -> None:
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 2, tzinfo=UTC)

    monkeypatch.setattr("backend.high_scale.rum.datetime", FrozenDatetime)
    client = ChartClient()
    result = rum_analytics(service_for(client), start, end)
    assert len(result["trends"]["timestamps"]) == 25
    for sql, params in client.calls:
        assert "event_timestamp >=" in sql
        assert "event_timestamp <=" in sql
        assert params["start"] == datetime(2026, 9, 1, tzinfo=UTC)
        assert params["end"] == datetime(2026, 9, 2, tzinfo=UTC)


def test_invalid_bounds_fail_before_query() -> None:
    client = ChartClient()
    with pytest.raises(ValueError, match="precedes"):
        rum_analytics(service_for(client), "2026-09-02T00:00:00Z", "2026-09-01T00:00:00Z")
    assert client.calls == []


@pytest.mark.parametrize("has_history", [False, True])
def test_empty_range_distinguishes_no_data_from_older_history(has_history: bool) -> None:
    class EmptyClient(ChartClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "rum_analytics_counts" in sql:
                return [{"beacons": 0, "pageviews": 0, "interactions": 0, "errors": 0}]
            if "rum_analytics_any_data" in sql:
                return [{"has_data": int(has_history)}]
            return []

    result = rum_analytics(service_for(EmptyClient()), "2026-09-01T00:00:00Z", "2026-09-01T02:00:00Z")
    assert result["no_data"] is not has_history
    assert (
        result["beacon_count"] == result["pageview_count"] == result["interaction_count"] == result["error_count"] == 0
    )
    assert (
        result["trends"]["lcp"]
        == result["trends"]["cls"]
        == result["trends"]["error_rate"]
        == ([None] * 3 if has_history else [])
    )
    assert result["trends"]["pageviews"] == ([0] * 3 if has_history else [])
    assert result["vitals"]["lcp"]["p75"] is None
    assert result["errors"] == result["worst_pages"] == []


def test_errors_only_and_null_metric_rows_remain_honest() -> None:
    class ErrorsOnlyClient(ChartClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "rum_analytics_counts" in sql:
                return [{"beacons": 1, "pageviews": 0, "interactions": 0, "errors": 1}]
            if "rum_analytics_vitals" in sql:
                return [{"metric_name": "lcp", "p75": None, "total": 0, "good": 0, "ni": 0, "poor": 0}]
            if "rum_analytics_trends" in sql:
                return [
                    {
                        "bucket": "2026-09-01T01:00:00Z",
                        "lcp": None,
                        "cls": None,
                        "views": 0,
                        "pageviews": 0,
                        "interactions": 0,
                        "errors": 1,
                    }
                ]
            return []

    result = rum_analytics(service_for(ErrorsOnlyClient()), "2026-09-01T00:00:00Z", "2026-09-01T02:00:00Z")
    assert result["no_data"] is False
    assert result["vitals"]["lcp"]["p75"] is None
    assert result["trends"]["lcp"] == result["trends"]["cls"] == [None, None, None]
    assert result["trends"]["error_rate"] == [None, 100.0, None]


# ── Merged from test_rum_fields.py ──

import pytest

from backend.high_scale.publication import rum_environment, rum_error_position


def test_flat_beacon_keys_are_extracted_verbatim() -> None:
    env = rum_environment(
        {
            "browser": "Firefox",
            "os": "Linux",
            "device": "Desktop",
            "country": "DE",
            "city": "Berlin",
            "region": "BE",
            "pop": "FRA",
            "tls": "1.3",
            "ttfb": 0.042,
        }
    )

    assert env == {
        "country": "DE",
        "browser": "Firefox",
        "os": "Linux",
        "device": "Desktop",
        "city": "Berlin",
        "region": "BE",
        "pop": "FRA",
        "tls": "1.3",
        "ttfb": 0.042,
    }


def test_edge_log_aliases_are_accepted_for_geography_and_connection_fields() -> None:
    env = rum_environment(
        {
            "geo_country_code": "FR",
            "geo_city": "Paris",
            "geo_region": "IDF",
            "server_pop": "CDG",
            "tls_version": "1.2",
            "time_to_first_byte": "88",
        }
    )

    assert env["country"] == "FR" and env["city"] == "Paris" and env["region"] == "IDF"
    assert env["pop"] == "CDG" and env["tls"] == "1.2" and env["ttfb"] == 88.0


def test_missing_values_stay_empty_and_are_never_defaulted_to_a_browser() -> None:
    env = rum_environment({"browser": "  ", "os": None, "ttfb": "not-a-number"})

    assert env["browser"] == "" and env["os"] == "" and env["device"] == ""
    assert env["country"] == "" and env["pop"] == "" and env["tls"] == ""
    assert env["ttfb"] is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True, ""])
def test_non_finite_or_non_numeric_ttfb_is_dropped(bad) -> None:
    assert rum_environment({"ttfb": bad})["ttfb"] is None


def test_numeric_strings_in_text_fields_are_kept_as_text() -> None:
    assert rum_environment({"tls": 1.3, "region": 13})["tls"] == "1.3"
    assert rum_environment({"region": 13})["region"] == "13"


def test_error_position_reads_the_rum_prefixed_or_plain_keys() -> None:
    assert rum_error_position({"rum_error_line": "42", "rum_error_col": 7}) == (42, 7)
    assert rum_error_position({"error_line": 5, "error_col": "9"}) == (5, 9)


@pytest.mark.parametrize("row", [{}, {"rum_error_line": "abc"}, {"rum_error_line": -3, "rum_error_col": None}])
def test_unknown_or_invalid_error_positions_are_zero(row) -> None:
    assert rum_error_position(row) == (0, 0)


# ── Merged from test_rum_router_filters.py ──

import json
from unittest.mock import PropertyMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.high_scale.rum import parse_rum_filters
from tests.conftest import override_request_context

_SVC = "svcA"
_FILTERS = {"browser": {"mode": "include", "values": ["Chrome"]}}


@pytest.fixture
def client():
    from backend.core.request_context import build_request_context
    from backend.routers import rum

    app = FastAPI()
    app.include_router(rum.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    with TestClient(app) as c:
        yield c


class _Service:
    service_id = _SVC
    client: Any = None


@pytest.fixture
def high_scale():
    with patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs:
        hs.return_value = _Service()
        yield hs


def test_parse_rum_filters_accepts_objects_and_rejects_everything_else() -> None:
    assert parse_rum_filters(None) == {} and parse_rum_filters("") == {}
    assert parse_rum_filters(json.dumps(_FILTERS)) == _FILTERS
    with pytest.raises(ValueError, match="valid JSON"):
        parse_rum_filters("{not json")
    with pytest.raises(ValueError, match="JSON object"):
        parse_rum_filters("[1, 2]")


def test_analytics_route_passes_the_parsed_filters_through(client, high_scale) -> None:
    seen: list[Any] = []

    def fake(service, start, end, filters=None):
        seen.append(filters)
        return {"is_mock": False, "no_data": True}

    with patch("backend.high_scale.rum.rum_analytics", fake):
        client.get(f"/api/services/{_SVC}/rum/analytics", params={"filters": json.dumps(_FILTERS)})

    assert seen == [_FILTERS]


def test_live_events_route_passes_the_parsed_filters_through(client, high_scale) -> None:
    seen: list[Any] = []

    def fake(service, start, end, limit, filters=None):
        seen.append(filters)
        return []

    with patch("backend.high_scale.rum.rum_live_events", fake):
        response = client.get(f"/api/services/{_SVC}/rum/live-events", params={"filters": json.dumps(_FILTERS)})

    assert response.status_code == 200 and seen == [_FILTERS]


@pytest.mark.parametrize("path", ["rum/analytics", "rum/live-events"])
@pytest.mark.parametrize("filters", ['{"metric_name": {"mode": "include", "values": ["LCP"]}}', "{broken"])
def test_unsupported_or_malformed_filters_are_a_client_error_not_a_server_error(
    client, high_scale, path, filters
) -> None:
    class _Client:
        def execute(self, sql, params=None):
            raise AssertionError("no query may run for a rejected filter")

    _Service.client = _Client()

    response = client.get(f"/api/services/{_SVC}/{path}", params={"filters": filters})

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "rum_filters_unsupported"


# ── Merged from test_rum_environment.py ──────────────────────────────


from backend.high_scale.rum import rum_live_events

START = "2026-09-01T00:00:00Z"
END = "2026-09-01T02:00:00Z"


class _EnvClient:
    def __init__(self, *, live_rows: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._live_rows = live_rows or []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "rum_analytics_counts" in sql:
            return [{"beacons": 3, "pageviews": 2, "interactions": 0, "errors": 1}]
        if "rum_analytics_environments" in sql:
            return [
                {
                    "env_browser": "Chrome",
                    "env_os": "Unknown",
                    "env_device": "Unknown",
                    "beacons": 5,
                    "g_browser": 0,
                    "g_os": 1,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "Unknown",
                    "env_device": "Unknown",
                    "beacons": 1,
                    "g_browser": 0,
                    "g_os": 1,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "macOS",
                    "env_device": "Unknown",
                    "beacons": 4,
                    "g_browser": 1,
                    "g_os": 0,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "Unknown",
                    "env_device": "Desktop",
                    "beacons": 6,
                    "g_browser": 1,
                    "g_os": 1,
                    "g_device": 0,
                },
            ]
        if "rum_analytics_exceptions" in sql:
            return [
                {"error_message": "boom", "error_file": "app.js", "error_line": 12, "error_col": 34, "count": 4},
                {"error_message": "oops", "error_file": "", "error_line": 0, "error_col": 0, "count": 1},
            ]
        if "rum_live_events" in sql:
            return self._live_rows
        return []

    def sql(self, marker: str) -> tuple[str, dict[str, Any]]:
        return next((sql, params) for sql, params in self.calls if marker in sql)


def _env_service(client: Any) -> HighScaleService:
    return HighScaleService(
        "TestRum", client, b"secret", ServingWatermark("TestRum", "request", 0, None, None, None, None, None, True)
    )


def test_environments_are_counted_per_distinct_beacon_from_the_vitals_facts() -> None:
    client = _EnvClient()

    result = rum_analytics(_env_service(client), START, END)

    assert result["environments"] == {
        "browsers": {"Chrome": 5, "Unknown": 1},
        "os": {"macOS": 4},
        "devices": {"Desktop": 6},
    }
    sql, _ = client.sql("rum_analytics_environments")
    assert "FROM rum_vitals_facts" in sql
    assert "GROUPING SETS" in sql and "uniqExact(" in sql
    assert "if(browser = '', 'Unknown', browser)" in sql


def test_blank_environment_values_are_reported_as_unknown_like_the_standard_path() -> None:
    class Blank(_EnvClient):
        def execute(self, sql, params=None):
            if "rum_analytics_environments" in sql:
                return [
                    {
                        "env_browser": "Unknown",
                        "env_os": "Unknown",
                        "env_device": "Unknown",
                        "beacons": 2,
                        "g_browser": 0,
                        "g_os": 1,
                        "g_device": 1,
                    }
                ]
            return super().execute(sql, params)

    assert rum_analytics(_env_service(Blank()), START, END)["environments"]["browsers"] == {"Unknown": 2}


def test_top_exceptions_carry_their_real_line_and_column() -> None:
    result = rum_analytics(_env_service(_EnvClient()), START, END)

    assert result["errors"] == [
        {"message": "boom", "file": "app.js", "line": 12, "col": 34, "count": 4},
        {"message": "oops", "file": "unknown.js", "line": 0, "col": 0, "count": 1},
    ]
    client = _EnvClient()
    rum_analytics(_env_service(client), START, END)
    sql, _ = client.sql("rum_analytics_exceptions")
    assert "error_line" in sql and "error_col" in sql
    assert "GROUP BY error_message, error_file, error_line, error_col" in sql


def test_filters_apply_to_every_analytics_query() -> None:
    client = _EnvClient()

    rum_analytics(
        _env_service(client),
        START,
        END,
        filters={
            "browser": {"mode": "include", "values": ["Chrome", "Firefox"]},
            "country": {"mode": "exclude", "values": ["RU"]},
        },
    )

    data_queries = [(sql, params) for sql, params in client.calls if "rum_" in sql and "any_data" not in sql]
    assert len(data_queries) >= 5
    for sql, params in data_queries:
        assert "browser IN ({flt_0_0:String}, {flt_0_1:String})" in sql
        assert "country NOT IN ({flt_1_0:String})" in sql
        assert params["flt_0_0"] == "Chrome" and params["flt_0_1"] == "Firefox" and params["flt_1_0"] == "RU"


def test_filters_rum_facts_cannot_express_fail_loudly() -> None:
    with pytest.raises(ValueError, match="cannot filter on"):
        rum_analytics(
            _env_service(_EnvClient()), START, END, filters={"metric_name": {"mode": "include", "values": ["LCP"]}}
        )


def test_blank_filters_are_ignored() -> None:
    client = _EnvClient()

    rum_analytics(_env_service(client), START, END, filters={"browser": {"mode": "include", "values": []}})

    for sql, _ in client.calls:
        assert "browser IN" not in sql


LIVE_ROWS = [
    {
        "timestamp": datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
        "type": "pageview",
        "pathname": "/checkout",
        "metric_name": "LCP",
        "metric_value": 2400.0,
        "metric_rating": "good",
        "browser": "Firefox",
        "os": "Linux",
        "device": "Desktop",
        "cid": "client-1",
        "req_id": "req-1",
        "error_message": "",
        "city": "Berlin",
        "region": "BE",
        "country": "DE",
        "pop": "FRA",
        "tls": "1.3",
        "ttfb": 0.042,
    },
    {
        "timestamp": "2026-09-01 01:29:00.000",
        "type": "error",
        "pathname": "",
        "metric_name": "",
        "metric_value": None,
        "metric_rating": "",
        "browser": "",
        "os": "",
        "device": "",
        "cid": "client-2",
        "req_id": "",
        "error_message": "TypeError: x is undefined",
        "city": "",
        "region": "",
        "country": "",
        "pop": "",
        "tls": "",
        "ttfb": None,
    },
]


def test_live_events_carry_the_beacon_environment_instead_of_placeholders() -> None:
    client = _EnvClient(live_rows=LIVE_ROWS)

    events = rum_live_events(_env_service(client), START, END, 50)

    first = events[0]
    assert first["type"] == "pageview" and first["path"] == "/checkout"
    assert first["browser"] == "Firefox" and first["os"] == "Linux"
    assert first["desc"] == "Metric LCP: 2400.0 (GOOD)"
    assert first["time"] == "2026-09-01T01:30:00Z"
    raw = first["raw_log"]
    assert raw["meta"] == {
        "browser": {"name": "Firefox"},
        "os": {"name": "Linux"},
        "device": {"type": "Desktop"},
        "page": {"url": "/checkout"},
    }
    assert (raw["city"], raw["region"], raw["country"], raw["pop"], raw["tls"]) == ("Berlin", "BE", "DE", "FRA", "1.3")
    assert raw["ttfb"] == 42
    assert raw["cid"] == "client-1" and raw["req_id"] == "req-1"
    assert raw["measurements"] == [{"type": "web-vitals", "values": {"LCP": 2400.0}, "context": {"rating": "good"}}]


def test_live_error_events_report_unknown_only_for_genuinely_missing_values() -> None:
    events = rum_live_events(_env_service(_EnvClient(live_rows=LIVE_ROWS)), START, END, 50)

    error = events[1]
    assert error["type"] == "error" and error["desc"] == "TypeError: x is undefined"
    assert error["time"] == "2026-09-01T01:29:00.000Z"
    assert error["browser"] == "Unknown" and error["path"] == "/"
    raw = error["raw_log"]
    assert raw["exceptions"] == [{"type": "Error", "value": "TypeError: x is undefined"}]
    assert raw["city"] == "Unknown" and raw["ttfb"] is None


def test_live_events_query_is_bounded_visible_and_parameterised() -> None:
    client = _EnvClient(live_rows=[])

    rum_live_events(_env_service(client), START, END, 25, filters={"os": {"mode": "include", "values": ["Linux"]}})

    sql, params = client.sql("rum_live_events")
    assert "FROM rum_vitals_facts" in sql and "FROM rum_error_facts" in sql
    assert "publication_state='visible'" in sql
    assert "high_scale_batch_publications FINAL" in sql
    assert "domain='rum_vitals'" in sql and "domain='rum_errors'" in sql
    # ClickHouse binds a trailing ORDER BY/LIMIT to the last SELECT only, so the union must be wrapped.
    assert "SELECT * FROM (" in sql and sql.index("SELECT * FROM (") < sql.index("UNION ALL")
    assert sql.index("UNION ALL") < sql.index(")\n    ORDER BY timestamp DESC")
    assert "LIMIT {limit:UInt32}" in sql and params["limit"] == 25
    assert sql.count("os IN ({flt_0_0:String})") == 2 and params["flt_0_0"] == "Linux"


def test_live_event_query_failures_propagate() -> None:
    class Broken(_EnvClient):
        def execute(self, sql, params=None):
            raise RuntimeError("clickhouse unavailable")

    with pytest.raises(RuntimeError, match="unavailable"):
        rum_live_events(_env_service(Broken()), START, END, 50)
