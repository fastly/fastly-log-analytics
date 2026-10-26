"""Regression coverage for the native RUM chart producer."""

from __future__ import annotations

import re
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


# ── Merged from tests/high_scale/test_rum_integration.py ──
import os
import subprocess
from collections.abc import Callable, Iterator
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from uuid import uuid4

import httpx

from backend.config import ClickHouseConfig
from backend.core.clickhouse_client import ClickHouseClient

_skip_unless_real_ch = pytest.mark.skipif(
    os.getenv("RUN_RUM_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse RUM gate"
)


def _ch_fact(
    ts: str,
    *,
    request_id: str | None = None,
    client_id: str = "fixture-client",
    service_id: str = "TestRum",
    state: str = "visible",
    **fields: Any,
) -> dict[str, Any]:
    return {
        "service_id": service_id,
        "event_id": str(uuid4()),
        "event_timestamp": ts,
        "ingest_timestamp": ts,
        "source_object_key": "raw/fixture.gz",
        "source_object_version": "fixture",
        "line_ordinal": 0,
        "transform_version": "fixture",
        "batch_id": str(uuid4()),
        "publication_state": state,
        "client_id": client_id,
        "request_event_id": request_id,
        "pathname": "/",
        "country": "US",
        **fields,
    }


def _ch_vital(ts: str, metric: str, value: float, **kwargs: Any) -> dict[str, Any]:
    return _ch_fact(ts, metric_name=metric, metric_value=value, metric_rating="good", **kwargs)


def _ch_error(ts: str, **kwargs: Any) -> dict[str, Any]:
    return _ch_fact(ts, **{"error_message": "fixture exception", "error_file": "fixture.js", **kwargs})


@pytest.fixture
def run_analytics_and_live(run_analytics: Callable[..., Any]) -> Callable[..., Any]:
    def run(vitals: list[dict[str, Any]], errors: list[dict[str, Any]]) -> Any:
        return run_analytics(vitals, errors, live=True)

    return run


@pytest.fixture
def run_analytics() -> Iterator[Callable[..., dict[str, Any]]]:
    clients: list[ClickHouseClient] = []

    def run(
        vitals: list[dict[str, Any]],
        errors: list[dict[str, Any]],
        start: str | None = "2026-09-01T00:00:00Z",
        end: str | None = "2026-09-01T02:00:00Z",
        filters: dict[str, Any] | None = None,
        live: bool = False,
    ) -> Any:
        schema = Path("backend/high_scale/sql/rum_schema.sql").read_text()
        schema = re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", schema)
        schema = re.sub(r"\nTTL [^;]*;", ";", schema)
        publication_schema = Path("backend/high_scale/sql/publication_schema.sql").read_text()
        publication_schema = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\s*\('[^']+',\s*'[^']+'\)",
            r"ReplacingMergeTree(\1)",
            publication_schema,
        )
        seed = publication_schema + "\n" + schema
        for table, domain, rows in (
            ("rum_vitals_facts", "rum_vitals", vitals),
            ("rum_error_facts", "rum_errors", errors),
        ):
            if rows:
                seed += f"\nINSERT INTO {table} FORMAT JSONEachRow\n"
                seed += "\n".join(json.dumps(row) for row in rows) + "\n;\n"
                seed += "\nINSERT INTO high_scale_batch_publications FORMAT JSONEachRow\n"
                seed += (
                    "\n".join(
                        json.dumps(
                            {
                                "batch_id": row["batch_id"],
                                "service_id": row["service_id"],
                                "domain": domain,
                                "generation": "1",
                                "batch_digest": "fixture",
                                "expected_rows": 1,
                                "visible_rows": 1,
                                "quorum_acked": 1,
                                "publication_state": "visible",
                                "manifest_version": 2,
                                "updated_at": row["ingest_timestamp"],
                            }
                        )
                        for row in rows
                    )
                    + "\n;\n"
                )

        def transport(request: httpx.Request) -> httpx.Response:
            form = BytesParser(policy=default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode() + request.read()
            )
            fields: dict[str, str] = {}
            for part in form.iter_parts():
                name = part.get_param("name", header="content-disposition")
                payload = part.get_payload(decode=True)
                assert isinstance(name, str) and isinstance(payload, bytes)
                fields[name] = payload.decode()
            command = [
                "docker",
                "run",
                "--rm",
                "-i",
                "--entrypoint",
                "clickhouse",
                os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13"),
                "local",
                "--multiquery",
                "--format=JSON",
                "--date_time_input_format=best_effort",
                "--output_format_json_quote_64bit_integers=0",
            ]
            command += [f"--{name}={value}" for name, value in fields.items() if name.startswith("param_")]
            completed = subprocess.run(
                command,
                input=seed + "\n" + fields["query"] + ";",
                text=True,
                capture_output=True,
                timeout=60,
                check=True,
            )
            return httpx.Response(200, content=completed.stdout.encode())

        settings = ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password="")
        client = ClickHouseClient(settings, transport=httpx.MockTransport(transport))
        clients.append(client)
        watermark = ServingWatermark("TestRum", "request", 0, None, None, None, None, None, True)
        service = HighScaleService("TestRum", client, b"test-secret", watermark)
        if live:
            return rum_live_events(service, start, end, 50, filters)
        return rum_analytics(service, start, end, filters)

    yield run
    for client in clients:
        client.close()


@_skip_unless_real_ch
def test_real_sql_distinct_counts_inclusive_bounds_and_missing_buckets(run_analytics) -> None:
    shared_id = str(uuid4())
    ts = "2026-09-01T00:00:00Z"
    result = run_analytics(
        [
            _ch_vital(ts, "LCP", 1000, request_id=shared_id),
            _ch_vital(ts, "CLS", 0.1, request_id=shared_id),
            _ch_vital(ts, "event_click", 1, request_id=shared_id),
            _ch_vital(ts, "LCP", 5000, client_id="second"),
            _ch_vital("2026-09-01T02:00:00Z", "CLS", 0.2),
            _ch_vital("2026-09-01T02:00:00.001Z", "LCP", 9000),
            _ch_vital(ts, "LCP", 99999, state="pending"),
            _ch_vital(ts, "LCP", 99999, service_id="TestOther"),
        ],
        [
            _ch_error(ts, request_id=shared_id),
            _ch_error(ts, request_id=shared_id),
        ],
    )
    assert result["no_data"] is False
    assert result["beacon_count"] == 3 and result["pageview_count"] == 3
    assert result["interaction_count"] == 1 and result["error_count"] == 1
    assert result["vitals"]["lcp"]["p75"] == 4.0
    trends = result["trends"]
    assert trends["lcp"] == [4.0, None, None] and trends["cls"] == [0.1, None, 0.2]
    assert trends["pageviews"] == [2, 0, 1] and trends["interactions"] == [1, 0, 0]
    assert trends["errors"] == [1, 0, 0] and trends["error_rate"] == [33.33, None, 0.0]
    assert result["worst_pages"][0]["views"] == 3 and result["worst_pages"][0]["lcp_p75"] == 4.0
    assert result["worst_pages"][0]["error_rate"] == 25.0
    assert result["errors"] == [{"message": "fixture exception", "file": "fixture.js", "line": 0, "col": 0, "count": 2}]
    assert result["environments"] == {"browsers": {"Unknown": 3}, "os": {"Unknown": 3}, "devices": {"Unknown": 3}}


@_skip_unless_real_ch
def test_real_sql_environments_error_positions_and_filters(run_analytics) -> None:
    ts = "2026-09-01T00:30:00Z"
    shared = str(uuid4())
    vitals = [
        _ch_vital(ts, "LCP", 1000, request_id=shared, browser="Chrome", os="macOS", device="Desktop", country="US"),
        _ch_vital(ts, "CLS", 0.1, request_id=shared, browser="Chrome", os="macOS", device="Desktop", country="US"),
        _ch_vital(ts, "LCP", 2000, client_id="b", browser="Firefox", os="Linux", device="Desktop", country="DE"),
        _ch_vital(ts, "LCP", 3000, client_id="c", browser="Chrome", os="Android", device="Mobile", country="US"),
    ]
    errors = [
        _ch_error(ts, client_id="b", browser="Firefox", error_line=12, error_col=34, country="DE"),
        _ch_error(ts, client_id="b2", browser="Firefox", error_line=12, error_col=34, country="DE"),
        _ch_error(ts, client_id="c", browser="Chrome", error_line=7, error_col=1, country="US"),
    ]

    result = run_analytics(vitals, errors)
    assert result["environments"] == {
        "browsers": {"Chrome": 2, "Firefox": 1},
        "os": {"macOS": 1, "Linux": 1, "Android": 1},
        "devices": {"Desktop": 2, "Mobile": 1},
    }
    assert result["errors"][0] == {
        "message": "fixture exception",
        "file": "fixture.js",
        "line": 12,
        "col": 34,
        "count": 2,
    }
    assert {"line": 7, "col": 1} == {key: result["errors"][1][key] for key in ("line", "col")}

    german = run_analytics(vitals, errors, filters={"country": {"mode": "include", "values": ["DE"]}})
    assert german["environments"]["browsers"] == {"Firefox": 1}
    assert german["error_count"] == 2 and german["pageview_count"] == 1

    not_chrome = run_analytics(vitals, errors, filters={"browser": {"mode": "exclude", "values": ["Chrome"]}})
    assert not_chrome["environments"]["browsers"] == {"Firefox": 1} and not_chrome["error_count"] == 2


@_skip_unless_real_ch
def test_real_sql_live_events_carry_real_environment_fields(run_analytics_and_live) -> None:
    ts = "2026-09-01T00:30:00Z"
    events = run_analytics_and_live(
        [
            _ch_vital(
                ts,
                "LCP",
                2400,
                browser="Firefox",
                os="Linux",
                device="Desktop",
                city="Berlin",
                region="BE",
                pop="FRA",
                tls="1.3",
                ttfb=0.042,
                country="DE",
            )
        ],
        [_ch_error("2026-09-01T00:29:00Z", error_message="boom")],
    )
    assert [event["type"] for event in events] == ["pageview", "error"]
    first = events[0]
    assert first["browser"] == "Firefox" and first["os"] == "Linux"
    assert first["raw_log"]["city"] == "Berlin" and first["raw_log"]["pop"] == "FRA" and first["raw_log"]["ttfb"] == 42
    assert first["desc"] == "Metric LCP: 2400.0 (GOOD)"
    assert events[1]["browser"] == "Unknown" and events[1]["desc"] == "boom"


@_skip_unless_real_ch
@pytest.mark.parametrize("hours,points", [(48, 49), (49, 3), (24 * 30, 31)])
def test_real_sql_daily_buckets_aggregate_all_hours(run_analytics, hours: int, points: int) -> None:
    from datetime import timedelta

    start = datetime(2026, 9, 1, tzinfo=UTC)
    result = run_analytics(
        [
            _ch_vital("2026-09-01T00:00:00Z", "lcp", 1000),
            _ch_vital("2026-09-01T23:00:00Z", "lcp", 5000),
        ],
        [_ch_error("2026-09-02T00:00:00Z")],
        start.isoformat(),
        (start + timedelta(hours=hours)).isoformat(),
    )
    trends = result["trends"]
    assert all(len(values) == points for values in trends.values())
    assert trends["timestamps"] == sorted(trends["timestamps"])
    assert trends["lcp"][0] == (1.0 if hours == 48 else 4.0)
    assert sum(trends["pageviews"]) == 2 and sum(trends["errors"]) == 1
    errors_only_bucket = 24 if hours == 48 else 1
    assert trends["lcp"][errors_only_bucket] is None and trends["cls"][errors_only_bucket] is None
    assert trends["error_rate"][errors_only_bucket] == 100.0


@_skip_unless_real_ch
def test_real_sql_errors_only_is_not_onboarding(run_analytics) -> None:
    result = run_analytics([], [_ch_error("2026-09-01T01:00:00Z")])
    assert result["no_data"] is False
    assert result["beacon_count"] == result["error_count"] == 1
    assert result["pageview_count"] == result["interaction_count"] == 0
    assert result["trends"]["lcp"] == result["trends"]["cls"] == [None, None, None]
    assert result["trends"]["error_rate"] == [None, 100.0, None]
    assert result["errors"] and result["worst_pages"] == []


@_skip_unless_real_ch
@pytest.mark.parametrize("older_data", [False, True])
def test_real_sql_empty_range_and_never_configured(run_analytics, older_data: bool) -> None:
    result = run_analytics([_ch_vital("2026-08-01T00:00:00Z", "LCP", 1000)] if older_data else [], [])
    assert result["no_data"] is not older_data and result["beacon_count"] == 0
    assert result["trends"]["lcp"] == ([None] * 3 if older_data else [])
    assert result["trends"]["pageviews"] == ([0] * 3 if older_data else [])
    assert result["vitals"]["lcp"]["p75"] is None and result["worst_pages"] == result["errors"] == []


@_skip_unless_real_ch
def test_real_sql_utc_offsets_and_partial_hour_edges(run_analytics) -> None:
    result = run_analytics(
        [
            _ch_vital("2026-09-01T00:14:59.999Z", "LCP", 9999),
            _ch_vital("2026-09-01T00:15:00Z", "LCP", 2000),
            _ch_vital("2026-09-01T01:15:00Z", "LCP", 3000),
            _ch_vital("2026-09-01T01:15:00.001Z", "LCP", 9999),
        ],
        [],
        "2026-08-31T18:15:00-06:00",
        "2026-08-31T19:15:00-06:00",
    )
    assert result["trends"]["timestamps"] == ["2026-09-01T00:00:00+00:00", "2026-09-01T01:00:00+00:00"]
    assert result["trends"]["lcp"] == [2.0, 3.0] and result["beacon_count"] == 2


@_skip_unless_real_ch
def test_real_sql_metric_formats_zero_and_case_normalization(run_analytics) -> None:
    ts = "2026-09-01T00:00:00Z"
    shared = str(uuid4())
    result = run_analytics(
        [
            _ch_vital(ts, "LCP", 1000, request_id=shared),
            _ch_vital(ts, "lcp", 5000, request_id=shared),
            _ch_vital(ts, "CLS", 0, request_id=shared),
            _ch_vital(ts, "INP", 123.9, request_id=shared),
            _ch_vital(ts, "FID", 12.34, request_id=shared),
            _ch_vital(ts, "FCP", 1500, request_id=shared),
            _ch_vital(ts, "TTFB", 0.5, request_id=shared),
        ],
        [],
    )
    vitals = result["vitals"]
    assert vitals["lcp"]["p75"] == result["trends"]["lcp"][0] == 4.0
    assert vitals["cls"]["p75"] == result["trends"]["cls"][0] == 0.0
    assert vitals["inp"]["p75"] == 123 and vitals["fid"] == {"p75": 12.3, "fcp": 1.5, "ttfb": 0.5}
    assert vitals["lcp"]["distribution"] == {"good": 100, "needs_improvement": 0, "poor": 0}
    assert result["beacon_count"] == result["pageview_count"] == 1


@_skip_unless_real_ch
def test_real_sql_interactions_only_keep_measurements_null(run_analytics) -> None:
    result = run_analytics([_ch_vital("2026-09-01T01:00:00Z", "event_click", 0)], [])
    assert result["beacon_count"] == result["interaction_count"] == 1
    assert result["pageview_count"] == result["error_count"] == 0
    assert result["trends"]["lcp"] == result["trends"]["cls"] == [None, None, None]
    assert result["trends"]["error_rate"] == [None, 0.0, None]


@_skip_unless_real_ch
def test_real_sql_empty_request_ids_use_client_timestamp_identity(run_analytics) -> None:
    ts = "2026-09-01T00:00:00Z"
    result = run_analytics(
        [
            _ch_vital(ts, "LCP", 1000),
            _ch_vital(ts, "CLS", 0.1),
        ],
        [_ch_error(ts)],
    )
    assert result["beacon_count"] == 1 and result["pageview_count"] == 1 and result["error_count"] == 1
    assert result["worst_pages"][0]["error_rate"] == 50.0


# ── Merged from tests/high_scale/test_late_rum_discovery.py ──────────────────

from types import SimpleNamespace
from unittest.mock import MagicMock

from backend.high_scale.source_discovery import (
    INITIAL_SOURCE_CURSOR,
    RUM_REINSPECTION_INTERVAL,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    S3SourceObjectLister,
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)
from botocore.stub import Stubber
from psycopg_pool import ConnectionPool

from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.postgres_control import PostgresControlPlane


class _DiscoveryPaginator:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.requests: list[dict[str, Any]] = []

    def paginate(self, **request: Any) -> list[dict[str, Any]]:
        self.requests.append(request)
        return [self.responses.pop(0)]


class _DiscoveryClient:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.paginator = _DiscoveryPaginator(responses)

    def get_paginator(self, name: str) -> _DiscoveryPaginator:
        assert name == "list_objects_v2"
        return self.paginator


def _disc_page(
    *, token: str | None = None, continuation_token: str | None = None, key: str | None = None
) -> dict[str, Any]:
    response: dict[str, Any] = {"Contents": []}
    if key is not None:
        response["Contents"] = [{"Key": key, "ETag": '"etag"', "Size": 1}]
    if token is not None:
        response["NextToken"] = token
    if continuation_token is not None:
        response["IsTruncated"] = True
        response["NextContinuationToken"] = continuation_token
    return response


def test_rum_cursor_reinspection_and_legacy_cursor_semantics() -> None:
    pages = [_disc_page(token=f"forward-{i}") for i in range(1, 11)] + [
        _disc_page(continuation_token="reinspect-1", key="raw/rum/page-1.gz")
    ]
    client = _DiscoveryClient(pages)
    lister = S3SourceObjectLister(client, bucket="archive-bucket")
    cursor: str | None = INITIAL_SOURCE_CURSOR
    reinspection_cursor = INITIAL_SOURCE_CURSOR
    forward_pages = 0

    for _ in range(RUM_REINSPECTION_INTERVAL):
        page = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=25,
            cursor=cursor,
            reinspection_cursor=reinspection_cursor,
            forward_pages_since_reinspection=forward_pages,
        )
        cursor = page.next_cursor
        reinspection_cursor = page.next_reinspection_cursor or reinspection_cursor
        forward_pages = page.forward_pages_since_reinspection or 0
    assert (cursor, reinspection_cursor, forward_pages) == (
        "forward-10",
        INITIAL_SOURCE_CURSOR,
        RUM_REINSPECTION_INTERVAL,
    )

    page = lister.list_source_objects(
        "svc",
        "rum_vitals",
        page_size=25,
        cursor=cursor,
        reinspection_cursor=reinspection_cursor,
        reinspect=True,
        forward_pages_since_reinspection=forward_pages,
    )
    assert page.next_cursor == cursor
    assert page.next_reinspection_cursor == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-1.gz"
    assert page.forward_pages_since_reinspection == 0
    assert client.paginator.requests[-1]["Prefix"] == "raw/rum/"
    assert client.paginator.requests[-1]["PaginationConfig"] == {"PageSize": 25}

    # Wrap to prefix start
    c2 = _DiscoveryClient([_disc_page(key="raw/rum/late-arrival.gz")])
    fwd_cur = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/z.gz"
    p2 = S3SourceObjectLister(c2, bucket="archive-bucket").list_source_objects(
        "svc",
        "rum_errors",
        page_size=25,
        cursor=fwd_cur,
        reinspection_cursor=INITIAL_SOURCE_CURSOR,
        reinspect=True,
        forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
    )
    assert [o.object_key for o in p2.objects] == ["raw/rum/late-arrival.gz"]
    assert (p2.next_cursor, p2.next_reinspection_cursor, p2.forward_pages_since_reinspection) == (
        fwd_cur,
        INITIAL_SOURCE_CURSOR,
        0,
    )

    # Legacy terminal cursor, paginator token, and request cursor
    c3 = _DiscoveryClient([_disc_page(key="raw/rum/new.gz")])
    p3 = S3SourceObjectLister(c3, bucket="archive-bucket").list_source_objects(
        "svc", "rum_vitals", page_size=25, cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/previous.gz"
    )
    assert c3.paginator.requests[0]["StartAfter"] == "raw/rum/previous.gz"
    assert p3.objects[0].object_key == "raw/rum/new.gz" and p3.next_cursor is None

    c4 = _DiscoveryClient([_disc_page(token="next-token")])
    p4 = S3SourceObjectLister(c4, bucket="archive-bucket").list_source_objects(
        "svc", "rum_errors", page_size=25, cursor="legacy-page-token"
    )
    assert c4.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25, "StartingToken": "legacy-page-token"}
    assert p4.next_cursor == "next-token"

    c5 = _DiscoveryClient([_disc_page(key="raw/request/new.gz")])
    S3SourceObjectLister(c5, bucket="archive-bucket").list_source_objects(
        "svc", "request", page_size=25, cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/previous.gz"
    )
    assert c5.paginator.requests[0]["StartAfter"] == "raw/request/previous.gz"


def test_rum_reinspection_continues_one_real_s3_page_at_a_time() -> None:
    import boto3

    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    stubber = Stubber(client)
    stubber.add_response(
        "list_objects_v2",
        {
            "IsTruncated": True,
            "Contents": [
                {"Key": "raw/rum/a.gz", "ETag": '"a"', "Size": 1},
                {"Key": "raw/rum/b.gz", "ETag": '"b"', "Size": 1},
            ],
            "NextContinuationToken": "opaque-continuation-token",
        },
        {"Bucket": "bucket", "Prefix": "raw/rum/", "MaxKeys": 2},
    )
    stubber.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "Contents": [{"Key": "raw/rum/c.gz", "ETag": '"c"', "Size": 1}]},
        {"Bucket": "bucket", "Prefix": "raw/rum/", "MaxKeys": 2, "StartAfter": "raw/rum/b.gz"},
    )
    lister = S3SourceObjectLister(client, bucket="bucket")
    with stubber:
        first = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=2,
            cursor="forward-cursor",
            reinspection_cursor=INITIAL_SOURCE_CURSOR,
            reinspect=True,
        )
        second = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=2,
            cursor="forward-cursor",
            reinspection_cursor=first.next_reinspection_cursor,
            reinspect=True,
        )
    assert [o.object_key for o in first.objects] == ["raw/rum/a.gz", "raw/rum/b.gz"]
    assert first.next_reinspection_cursor == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/b.gz"
    assert [o.object_key for o in second.objects] == ["raw/rum/c.gz"]
    assert second.next_reinspection_cursor == INITIAL_SOURCE_CURSOR
    assert first.next_cursor == second.next_cursor == "forward-cursor"


def test_coordinator_and_postgres_reinspection_cursor_persistence() -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(
        current_owner="high_scale", owner_epoch=3, source_cursor="owner-cursor"
    )
    control.source_cursor_for.return_value = forward_cursor
    control.source_reinspection_state_for.return_value = (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL)
    control.claim_sources_batch.return_value = ()
    control.expired_claims.return_value = ()

    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(
        (),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=MagicMock(),
        controller=MagicMock(),
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="w1",
        control_plane=control,
    )
    run = coordinator.run_page(service_id="svc", domain="rum_vitals")
    assert run.next_cursor == forward_cursor
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_called_once_with(
        "svc", "rum_vitals", INITIAL_SOURCE_CURSOR, 0, expected_owner="high_scale", expected_owner_epoch=3
    )

    # Forward + reinspection advance together
    control2 = MagicMock()
    control2.owner.return_value = SimpleNamespace(
        current_owner="high_scale", owner_epoch=3, source_cursor="owner-cursor"
    )
    control2.source_cursor_for.return_value = "forward-page-1"
    control2.source_reinspection_state_for.return_value = ("reinspection-page-2", 2)
    control2.claim_sources_batch.return_value = ()
    control2.expired_claims.return_value = ()
    lister2 = MagicMock()
    lister2.list_source_objects.return_value = SourceObjectPage(
        (), "forward-page-2", next_reinspection_cursor="reinspection-page-2", forward_pages_since_reinspection=3
    )
    HighScaleWorkerCoordinator(
        lister=lister2,
        reader=MagicMock(),
        controller=MagicMock(),
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="w2",
        control_plane=control2,
    ).run_page(service_id="svc", domain="rum_errors")
    control2.advance_source_cursor.assert_called_once_with(
        "svc",
        "forward-page-2",
        expected_owner="high_scale",
        expected_owner_epoch=3,
        domain="rum_errors",
        reinspection_cursor="reinspection-page-2",
        forward_pages_since_reinspection=3,
    )

    # PostgresControlPlane unit checks
    for row, expected in (
        (("__terminal__:raw/rum/a.gz", 4), ("__terminal__:raw/rum/a.gz", 4)),
        (None, (INITIAL_SOURCE_CURSOR, 0)),
    ):
        pool, conn, sel = MagicMock(), MagicMock(), MagicMock()
        pool.connection.return_value.__enter__.return_value = conn
        conn.transaction.return_value.__enter__.return_value = conn
        sel.fetchone.return_value = row
        conn.execute.side_effect = [MagicMock(), sel]
        assert PostgresControlPlane(pool=pool).source_reinspection_state_for("svc", "rum_vitals") == expected

    pool, conn, owner = MagicMock(), MagicMock(), MagicMock()
    pool.connection.return_value.__enter__.return_value = conn
    conn.transaction.return_value.__enter__.return_value = conn
    owner.fetchone.return_value = {
        "service_id": "svc",
        "owner_epoch": 3,
        "current_owner": "high_scale",
        "previous_owner": None,
        "source_cursor": "owner-cursor",
        "drain_complete": False,
        "cutover_committed": False,
        "rollback_allowed": False,
    }
    conn.execute.side_effect = [owner, MagicMock()]
    PostgresControlPlane(pool=pool).advance_reinspection_state(
        "svc", "rum_vitals", "reinspection-page-2", 0, expected_owner="high_scale", expected_owner_epoch=3
    )
    update_sql = conn.execute.call_args_list[1].args[0]
    assert "SET reinspection_cursor=%s" in update_sql and "source_cursor=" not in update_sql


@pytest.mark.parametrize("stage", ["read", "publication", "missing"])
def test_reinspection_failure_and_missing_object_cursor_handling(stage: str) -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    descriptor = SourceObjectDescriptor("raw/rum/late.gz", "etag:late", 12, "v1")
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(current_owner="high_scale", owner_epoch=5, source_cursor=None)
    control.source_cursor_for.return_value = forward_cursor
    control.source_reinspection_state_for.return_value = (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL)
    control.discover_source.return_value = SimpleNamespace(status="discovered")
    control.claim_sources_batch.return_value = (SimpleNamespace(object_key=descriptor.object_key),)
    control.expired_claims.return_value = ()
    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(
        (descriptor,),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    reader, controller = MagicMock(), MagicMock()
    if stage == "read":
        reader.read_source_object.side_effect = RuntimeError("temporary FOS read failure")
    elif stage == "publication":
        reader.read_source_object.return_value = b"source payload"
        controller.ingest_batch.side_effect = RuntimeError("temporary publication failure")
    else:
        reader.read_source_object.side_effect = SourceObjectMissingError("source disappeared")

    run = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="w",
        control_plane=control,
    ).run_page(service_id="svc", domain="rum_vitals")

    control.advance_source_cursor.assert_not_called()
    if stage == "missing":
        assert run.missing == 1 and run.failed == 0
        control.mark_source_missing.assert_called_once()
        control.advance_reinspection_state.assert_called_once_with(
            "svc", "rum_vitals", INITIAL_SOURCE_CURSOR, 0, expected_owner="high_scale", expected_owner_epoch=5
        )
    else:
        assert run.failed == 1
        control.advance_reinspection_state.assert_not_called()
        if stage == "publication":
            assert controller.ingest_batch.call_args.kwargs["next_cursor"] is None


@pytest.mark.postgres_integration
def test_postgres_rum_reinspection_state_is_schema_scoped_and_owner_fenced() -> None:
    dsn = os.getenv("HIGH_SCALE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("HIGH_SCALE_POSTGRES_TEST_DSN is not set")
    schema = f"hs_reinspect_{uuid4().hex}"
    setup_pool = ConnectionPool(conninfo=dsn, kwargs={"autocommit": False}, min_size=1, max_size=1, timeout=5)
    scoped_pool: ConnectionPool | None = None
    try:
        setup_pool.open(wait=True)
        with setup_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(f'CREATE SCHEMA "{schema}"')
        scoped_pool = ConnectionPool(
            conninfo=dsn,
            kwargs={"autocommit": False, "options": f"-csearch_path={schema}"},
            min_size=1,
            max_size=2,
            timeout=5,
        )
        scoped_pool.open(wait=True)
        control = PostgresControlPlane(pool=scoped_pool, pool_timeout=5)
        control.create_schema()
        owner = control.initialize_owner("svc", owner="high_scale", source_cursor="owner")
        latest = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
        control.advance_source_cursor(
            "svc",
            latest,
            expected_owner="high_scale",
            expected_owner_epoch=owner.owner_epoch,
            domain="rum_vitals",
            reinspection_cursor=INITIAL_SOURCE_CURSOR,
            forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
        )
        with scoped_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(
                "ALTER TABLE high_scale_source_cursors DROP COLUMN reinspection_cursor, DROP COLUMN forward_pages_since_reinspection"
            )
        control.create_schema()
        assert control.source_cursor_for("svc", "rum_vitals") == latest
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (INITIAL_SOURCE_CURSOR, 0)

        p2 = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-2.gz"
        control.advance_reinspection_state(
            "svc", "rum_vitals", p2, 0, expected_owner="high_scale", expected_owner_epoch=owner.owner_epoch
        )
        assert control.source_cursor_for("svc", "rum_vitals") == latest
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (p2, 0)
        control.advance_source_cursor(
            "svc", latest, expected_owner="high_scale", expected_owner_epoch=owner.owner_epoch, domain="rum_vitals"
        )
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (p2, 0)
        with pytest.raises(ValueError, match="owner epoch"):
            control.advance_reinspection_state(
                "svc",
                "rum_vitals",
                INITIAL_SOURCE_CURSOR,
                0,
                expected_owner="high_scale",
                expected_owner_epoch=owner.owner_epoch + 1,
            )
    finally:
        if scoped_pool is not None:
            scoped_pool.close()
        if setup_pool.closed:
            setup_pool.open(wait=True)
        with setup_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        setup_pool.close()
