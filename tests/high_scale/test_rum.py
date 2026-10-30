"""Regression coverage for the native RUM chart producer, fields, filters, environments, and late discovery."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch
from uuid import uuid4

import boto3
import httpx
import pytest
from botocore.stub import Stubber
from fastapi import FastAPI
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from backend.config import ClickHouseConfig
from backend.core.clickhouse import ClickHouseClient
from backend.core.request_context import build_request_context
from backend.high_scale.archive import (
    ServingWatermark,
    rum_environment,
    rum_error_position,
)
from backend.high_scale.ingest_controller import (
    INITIAL_SOURCE_CURSOR,
    RUM_REINSPECTION_INTERVAL,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    HighScaleService,
    HighScaleWorkerCoordinator,
    S3SourceObjectLister,
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.rum import parse_rum_filters, rum_analytics, rum_live_events
from backend.routers import rum as rum_router
from tests.conftest import override_request_context

_TREND_KEYS = ("bucket", "lcp", "cls", "views", "pageviews", "interactions", "errors")


class ChartClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "rum_analytics_counts" in sql:
            return [{"beacons": 3, "pageviews": 1, "interactions": 1, "errors": 1}]
        if "rum_analytics_trends" in sql:
            return [dict(zip(_TREND_KEYS, ("2026-09-01 00:00:00", 2500.0, 0.125, 2, 1, 1, 1), strict=True))]
        if "GROUP BY metric_name" in sql:
            return [{"metric_name": "LCP", "p75": 2500.0, "total": 1, "good": 1, "ni": 0, "poor": 0}]
        return [{"c": 1}] if " as c " in sql else []


def service_for(client: Any) -> HighScaleService:
    watermark = ServingWatermark("TestRum", "request", 0, None, None, None, None, None, True)
    return HighScaleService("TestRum", client, b"test-secret", watermark)


def test_real_rows_populate_aligned_chart_series() -> None:
    client = ChartClient()
    result = rum_analytics(service_for(client), "2026-09-01T00:15:00Z", "2026-09-01T02:15:00Z")
    trends = result["trends"]
    assert trends["timestamps"] == [f"2026-09-01T0{h}:00:00+00:00" for h in range(3)]
    assert trends["lcp"] == [2.5, None, None] and trends["cls"] == [0.125, None, None]
    assert trends["error_rate"] == [33.33, None, None]
    assert trends["pageviews"] == trends["interactions"] == trends["errors"] == [1, 0, 0]
    assert result["vitals"]["lcp"]["p75"] == 2.5
    assert result["beacon_count"] == 3 and result["interaction_count"] == 1
    for sql, params in client.calls:
        assert "publication_state" in sql and params["service_id"] == "TestRum"
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
        assert "event_timestamp >=" in sql and "event_timestamp <=" in sql
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
            return [{"has_data": int(has_history)}] if "rum_analytics_any_data" in sql else []

    res = rum_analytics(service_for(EmptyClient()), "2026-09-01T00:00:00Z", "2026-09-01T02:00:00Z")
    assert res["no_data"] is not has_history
    assert res["beacon_count"] == res["pageview_count"] == res["interaction_count"] == res["error_count"] == 0
    tr = res["trends"]
    assert tr["lcp"] == tr["cls"] == tr["error_rate"] == ([None] * 3 if has_history else [])
    assert tr["pageviews"] == ([0] * 3 if has_history else [])
    assert res["vitals"]["lcp"]["p75"] is None and res["errors"] == res["worst_pages"] == []


def test_errors_only_and_null_metric_rows_remain_honest() -> None:
    class ErrorsOnlyClient(ChartClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "rum_analytics_counts" in sql:
                return [{"beacons": 1, "pageviews": 0, "interactions": 0, "errors": 1}]
            if "rum_analytics_vitals" in sql:
                return [{"metric_name": "lcp", "p75": None, "total": 0, "good": 0, "ni": 0, "poor": 0}]
            if "rum_analytics_trends" in sql:
                return [dict(zip(_TREND_KEYS, ("2026-09-01T01:00:00Z", None, None, 0, 0, 0, 1), strict=True))]
            return []

    result = rum_analytics(service_for(ErrorsOnlyClient()), "2026-09-01T00:00:00Z", "2026-09-01T02:00:00Z")
    assert result["no_data"] is False and result["vitals"]["lcp"]["p75"] is None
    assert result["trends"]["lcp"] == result["trends"]["cls"] == [None, None, None]
    assert result["trends"]["error_rate"] == [None, 100.0, None]


# ── RUM field extraction ──
def test_flat_beacon_keys_and_edge_aliases_are_extracted() -> None:
    keys = ("browser", "os", "device", "country", "city", "region", "pop", "tls", "ttfb")
    flat = dict(zip(keys, ("Firefox", "Linux", "Desktop", "DE", "Berlin", "BE", "FRA", "1.3", 0.042), strict=True))
    assert rum_environment(flat) == flat

    alias_keys = ("geo_country_code", "geo_city", "geo_region", "server_pop", "tls_version", "time_to_first_byte")
    aliased = rum_environment(dict(zip(alias_keys, ("FR", "Paris", "IDF", "CDG", "1.2", "88"), strict=True)))
    assert (aliased["country"], aliased["city"], aliased["region"]) == ("FR", "Paris", "IDF")
    assert (aliased["pop"], aliased["tls"], aliased["ttfb"]) == ("CDG", "1.2", 88.0)


def test_missing_values_numeric_strings_and_non_finite_ttfb() -> None:
    env = rum_environment({"browser": "  ", "os": None, "ttfb": "not-a-number"})
    assert env["browser"] == env["os"] == env["device"] == env["country"] == env["pop"] == env["tls"] == ""
    assert env["ttfb"] is None
    for bad in (float("nan"), float("inf"), True, ""):
        assert rum_environment({"ttfb": bad})["ttfb"] is None
    assert rum_environment({"tls": 1.3, "region": 13})["tls"] == "1.3"
    assert rum_environment({"region": 13})["region"] == "13"


def test_error_position_extraction() -> None:
    assert rum_error_position({"rum_error_line": "42", "rum_error_col": 7}) == (42, 7)
    assert rum_error_position({"error_line": 5, "error_col": "9"}) == (5, 9)
    for row in ({}, {"rum_error_line": "abc"}, {"rum_error_line": -3, "rum_error_col": None}):
        assert rum_error_position(row) == (0, 0)


# ── Router filters ──
_SVC = "svcA"
_FILTERS = {"browser": {"mode": "include", "values": ["Chrome"]}}


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(rum_router.router)
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


def test_routes_pass_parsed_filters_through(client, high_scale) -> None:
    seen_analytics: list[Any] = []
    seen_live: list[Any] = []
    with (
        patch(
            "backend.high_scale.rum.rum_analytics",
            lambda s, st, et, filters=None: (seen_analytics.append(filters), {"is_mock": False, "no_data": True})[1],
        ),
        patch(
            "backend.high_scale.rum.rum_live_events",
            lambda s, st, et, lim, filters=None: (seen_live.append(filters), [])[1],
        ),
    ):
        client.get(f"/api/services/{_SVC}/rum/analytics", params={"filters": json.dumps(_FILTERS)})
        resp = client.get(f"/api/services/{_SVC}/rum/live-events", params={"filters": json.dumps(_FILTERS)})
    assert seen_analytics == [_FILTERS] and resp.status_code == 200 and seen_live == [_FILTERS]


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
    assert response.status_code == 400 and response.json()["detail"]["error"] == "rum_filters_unsupported"


# ── Environments & Live Events ──
START = "2026-09-01T00:00:00Z"
END = "2026-09-01T02:00:00Z"
_ENV_ROW_KEYS = ("env_browser", "env_os", "env_device", "beacons", "g_browser", "g_os", "g_device")


def _env_row(*vals: Any) -> dict[str, Any]:
    return dict(zip(_ENV_ROW_KEYS, vals, strict=True))


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
                _env_row("Chrome", "Unknown", "Unknown", 5, 0, 1, 1),
                _env_row("Unknown", "Unknown", "Unknown", 1, 0, 1, 1),
                _env_row("Unknown", "macOS", "Unknown", 4, 1, 0, 1),
                _env_row("Unknown", "Unknown", "Desktop", 6, 1, 1, 0),
            ]
        if "rum_analytics_exceptions" in sql:
            return [
                {"error_message": "boom", "error_file": "app.js", "error_line": 12, "error_col": 34, "count": 4},
                {"error_message": "oops", "error_file": "", "error_line": 0, "error_col": 0, "count": 1},
            ]
        return self._live_rows if "rum_live_events" in sql else []

    def sql(self, marker: str) -> tuple[str, dict[str, Any]]:
        return next((sql, params) for sql, params in self.calls if marker in sql)


def test_environments_are_counted_per_distinct_beacon_from_the_vitals_facts() -> None:
    client = _EnvClient()
    result = rum_analytics(service_for(client), START, END)
    assert result["environments"] == {
        "browsers": {"Chrome": 5, "Unknown": 1},
        "os": {"macOS": 4},
        "devices": {"Desktop": 6},
    }
    sql, _ = client.sql("rum_analytics_environments")
    assert "FROM rum_vitals_facts" in sql and "GROUPING SETS" in sql and "uniqExact(" in sql
    assert "if(browser = '', 'Unknown', browser)" in sql


def test_blank_environment_values_are_reported_as_unknown_like_the_standard_path() -> None:
    class Blank(_EnvClient):
        def execute(self, sql, params=None):
            if "rum_analytics_environments" in sql:
                return [_env_row("Unknown", "Unknown", "Unknown", 2, 0, 1, 1)]
            return super().execute(sql, params)

    assert rum_analytics(service_for(Blank()), START, END)["environments"]["browsers"] == {"Unknown": 2}


def test_top_exceptions_carry_their_real_line_and_column() -> None:
    client = _EnvClient()
    result = rum_analytics(service_for(client), START, END)
    assert result["errors"] == [
        {"message": "boom", "file": "app.js", "line": 12, "col": 34, "count": 4},
        {"message": "oops", "file": "unknown.js", "line": 0, "col": 0, "count": 1},
    ]
    sql, _ = client.sql("rum_analytics_exceptions")
    assert "error_line" in sql and "error_col" in sql
    assert "GROUP BY error_message, error_file, error_line, error_col" in sql


def test_filters_apply_to_every_analytics_query() -> None:
    client = _EnvClient()
    flts = {
        "browser": {"mode": "include", "values": ["Chrome", "Firefox"]},
        "country": {"mode": "exclude", "values": ["RU"]},
    }
    rum_analytics(service_for(client), START, END, filters=flts)
    data_queries = [(sql, params) for sql, params in client.calls if "rum_" in sql and "any_data" not in sql]
    assert len(data_queries) >= 5
    for sql, params in data_queries:
        assert "browser IN ({flt_0_0:String}, {flt_0_1:String})" in sql
        assert "country NOT IN ({flt_1_0:String})" in sql
        assert (params["flt_0_0"], params["flt_0_1"], params["flt_1_0"]) == ("Chrome", "Firefox", "RU")


def test_filters_rum_facts_cannot_express_and_blank_filters() -> None:
    with pytest.raises(ValueError, match="cannot filter on"):
        rum_analytics(
            service_for(_EnvClient()), START, END, filters={"metric_name": {"mode": "include", "values": ["LCP"]}}
        )
    client = _EnvClient()
    rum_analytics(service_for(client), START, END, filters={"browser": {"mode": "include", "values": []}})
    for sql, _ in client.calls:
        assert "browser IN" not in sql


_LIVE_STR_KEYS = (
    "pathname,metric_name,metric_rating,browser,os,device,req_id,city,region,country,pop,tls,error_message".split(",")
)
LIVE_ROWS = [
    dict(
        zip(
            _LIVE_STR_KEYS,
            (
                "/checkout",
                "LCP",
                "good",
                "Firefox",
                "Linux",
                "Desktop",
                "req-1",
                "Berlin",
                "BE",
                "DE",
                "FRA",
                "1.3",
                "",
            ),
            strict=True,
        ),
        timestamp=datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
        type="pageview",
        metric_value=2400.0,
        cid="client-1",
        ttfb=0.042,
    ),
    dict.fromkeys(_LIVE_STR_KEYS, "")
    | {
        "timestamp": "2026-09-01 01:29:00.000",
        "type": "error",
        "metric_value": None,
        "cid": "client-2",
        "error_message": "TypeError: x is undefined",
        "ttfb": None,
    },
]


def test_live_events_carry_the_beacon_environment_and_unknown_fallbacks() -> None:
    events = rum_live_events(service_for(_EnvClient(live_rows=LIVE_ROWS)), START, END, 50)

    first = events[0]
    assert (first["type"], first["path"], first["browser"], first["os"]) == (
        "pageview",
        "/checkout",
        "Firefox",
        "Linux",
    )
    assert first["desc"] == "Metric LCP: 2400.0 (GOOD)" and first["time"] == "2026-09-01T01:30:00Z"
    raw = first["raw_log"]
    assert raw["meta"] == {
        "browser": {"name": "Firefox"},
        "os": {"name": "Linux"},
        "device": {"type": "Desktop"},
        "page": {"url": "/checkout"},
    }
    assert (raw["city"], raw["region"], raw["country"], raw["pop"], raw["tls"]) == ("Berlin", "BE", "DE", "FRA", "1.3")
    assert raw["ttfb"] == 42 and raw["cid"] == "client-1" and raw["req_id"] == "req-1"
    assert raw["measurements"] == [{"type": "web-vitals", "values": {"LCP": 2400.0}, "context": {"rating": "good"}}]

    error = events[1]
    assert error["type"] == "error" and error["desc"] == "TypeError: x is undefined"
    assert error["time"] == "2026-09-01T01:29:00.000Z" and error["browser"] == "Unknown" and error["path"] == "/"
    assert error["raw_log"]["exceptions"] == [{"type": "Error", "value": "TypeError: x is undefined"}]
    assert error["raw_log"]["city"] == "Unknown" and error["raw_log"]["ttfb"] is None


def test_live_events_enrich_missing_geo_from_pop_and_ttfb_from_session() -> None:
    rows = [
        dict.fromkeys(_LIVE_STR_KEYS, "")
        | {
            "timestamp": datetime(2026, 9, 1, 1, 31, tzinfo=UTC),
            "pathname": "/cart",
            "type": "vital",
            "metric_name": "LCP",
            "metric_value": 1850.0,
            "metric_rating": "good",
            "browser": "Chrome",
            "os": "Mac OS",
            "device": "Desktop",
            "pop": "DEN",
            "tls": "Unknown",
            "cid": "session-den-1",
            "ttfb": 0.0,
        },
        dict.fromkeys(_LIVE_STR_KEYS, "")
        | {
            "timestamp": datetime(2026, 9, 1, 1, 30, 59, tzinfo=UTC),
            "pathname": "/cart",
            "type": "vital",
            "metric_name": "TTFB",
            "metric_value": 0.145,
            "metric_rating": "good",
            "browser": "Chrome",
            "os": "Mac OS",
            "device": "Desktop",
            "pop": "DEN",
            "tls": "1.3",
            "cid": "session-den-1",
            "ttfb": 0.0,
        },
    ]
    events = rum_live_events(service_for(_EnvClient(live_rows=rows)), START, END, 50)
    assert len(events) == 2
    lcp_ev, ttfb_ev = events[0], events[1]
    assert (
        lcp_ev["raw_log"]["city"],
        lcp_ev["raw_log"]["region"],
        lcp_ev["raw_log"]["country"],
        lcp_ev["raw_log"]["pop"],
    ) == ("Denver", "CO", "US", "DEN")
    assert lcp_ev["raw_log"]["tls"] == "Unknown"
    assert lcp_ev["raw_log"]["ttfb"] == 145
    assert ttfb_ev["raw_log"]["ttfb"] == 145


def test_parse_rum_line_extracts_pop_geo_fallback_and_query_params() -> None:
    from backend.core.rum_ingest import _parse_rum_line

    log_data = {
        "timestamp": "2026-10-29T12:00:00Z",
        "url": "https://edge.example.com/_rum/v1/beacon?rum_pathname=%2Fproducts&rum_cid=cid-99&req=req-abc",
        "pop": "MSP",
        "city": "unknown",
        "region": "?",
        "country": "",
        "tls": "TLSv1.3",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
        "rum_body": '{"measurements":[{"type":"web-vitals","values":{"LCP":1200.0,"TTFB":95.0}}]}',
    }
    res = _parse_rum_line(log_data, "svc-test")
    assert res is not None
    vitals, errors = res
    assert not errors
    assert len(vitals) == 2
    by_metric = {v["metric_name"]: v for v in vitals}
    lcp = by_metric["LCP"]
    assert (lcp["city"], lcp["region"], lcp["country"], lcp["pop"]) == ("Minneapolis", "MN", "US", "MSP")
    assert lcp["cid"] == "cid-99" and lcp["req_id"] == "req-abc" and lcp["pathname"] == "/products"
    assert lcp["ttfb"] == 95.0 and lcp["tls"] == "TLSv1.3"


def test_live_events_query_is_bounded_visible_and_parameterised() -> None:
    client = _EnvClient(live_rows=[])
    rum_live_events(service_for(client), START, END, 25, filters={"os": {"mode": "include", "values": ["Linux"]}})

    sql, params = client.sql("rum_live_events")
    assert "FROM rum_vitals_facts" in sql and "FROM rum_error_facts" in sql
    assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
    assert "domain='rum_vitals'" in sql and "domain='rum_errors'" in sql
    assert "SELECT * FROM (" in sql and sql.index("SELECT * FROM (") < sql.index("UNION ALL")
    assert sql.index("UNION ALL") < sql.index(")\n    ORDER BY timestamp DESC")
    assert "LIMIT {limit:UInt32}" in sql and params["limit"] == 25
    assert sql.count("os IN ({flt_0_0:String})") == 2 and params["flt_0_0"] == "Linux"


def test_live_event_query_failures_propagate() -> None:
    class Broken(_EnvClient):
        def execute(self, sql, params=None):
            raise RuntimeError("clickhouse unavailable")

    with pytest.raises(RuntimeError, match="unavailable"):
        rum_live_events(service_for(Broken()), START, END, 50)


# ── Real ClickHouse SQL integration ──
_skip_unless_real_ch = pytest.mark.skipif(
    os.getenv("RUN_RUM_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse RUM gate"
)


def _ch_fact(ts: str, **fields: Any) -> dict[str, Any]:
    return {
        "service_id": fields.pop("service_id", "TestRum"),
        "event_id": str(uuid4()),
        "event_timestamp": ts,
        "ingest_timestamp": ts,
        "source_object_key": "raw/fixture.gz",
        "source_object_version": "fixture",
        "line_ordinal": 0,
        "transform_version": "fixture",
        "batch_id": str(uuid4()),
        "publication_state": fields.pop("state", "visible"),
        "client_id": fields.pop("client_id", "fixture-client"),
        "request_event_id": fields.pop("request_id", None),
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
    return lambda vitals, errors: run_analytics(vitals, errors, live=True)


@pytest.fixture
def run_analytics() -> Iterator[Callable[..., dict[str, Any]]]:
    clients: list[ClickHouseClient] = []
    pub_keys = (
        "batch_id,service_id,domain,generation,batch_digest,expected_rows,"
        "visible_rows,quorum_acked,publication_state,manifest_version,updated_at"
    ).split(",")

    def run(
        vitals: list[dict[str, Any]],
        errors: list[dict[str, Any]],
        start: str | None = "2026-09-01T00:00:00Z",
        end: str | None = "2026-09-01T02:00:00Z",
        filters: dict[str, Any] | None = None,
        live: bool = False,
    ) -> Any:
        raw_rum = Path("backend/high_scale/sql/rum_schema.sql").read_text()
        schema = re.sub(r"\nTTL [^;]*;", ";", re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", raw_rum))
        pub_schema = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\s*\('[^']+',\s*'[^']+'\)",
            r"ReplacingMergeTree(\1)",
            Path("backend/high_scale/sql/publication_schema.sql").read_text(),
        )
        seed = pub_schema + "\n" + schema
        for table, domain, rows in (
            ("rum_vitals_facts", "rum_vitals", vitals),
            ("rum_error_facts", "rum_errors", errors),
        ):
            if rows:
                seed += f"\nINSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows) + "\n;\n"
                pubs = [
                    dict(
                        zip(
                            pub_keys,
                            (
                                r["batch_id"],
                                r["service_id"],
                                domain,
                                "1",
                                "fixture",
                                1,
                                1,
                                1,
                                "visible",
                                2,
                                r["ingest_timestamp"],
                            ),
                            strict=True,
                        )
                    )
                    for r in rows
                ]
                seed += (
                    "\nINSERT INTO high_scale_batch_publications FORMAT JSONEachRow\n"
                    + "\n".join(json.dumps(p) for p in pubs)
                    + "\n;\n"
                )

        def transport(request: httpx.Request) -> httpx.Response:
            form = BytesParser(policy=default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode() + request.read()
            )
            fields = {
                part.get_param("name", header="content-disposition"): part.get_payload(decode=True).decode()
                for part in form.iter_parts()
            }
            img = os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")
            cmd = [
                "docker", "run", "--rm", "-i", "--entrypoint", "clickhouse", img, "local", "--multiquery",
                "--format=JSON", "--date_time_input_format=best_effort", "--output_format_json_quote_64bit_integers=0",
                *[f"--{k}={v}" for k, v in fields.items() if isinstance(k, str) and k.startswith("param_")],
            ]  # fmt: skip
            res = subprocess.run(
                cmd, input=f"{seed}\n{fields['query']};", text=True, capture_output=True, timeout=60, check=True
            )
            return httpx.Response(200, content=res.stdout.encode())

        cfg = ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password="")
        ch_client = ClickHouseClient(cfg, transport=httpx.MockTransport(transport))
        clients.append(ch_client)
        svc = service_for(ch_client)
        return rum_live_events(svc, start, end, 50, filters) if live else rum_analytics(svc, start, end, filters)

    yield run
    for c in clients:
        c.close()


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
        [_ch_error(ts, request_id=shared_id), _ch_error(ts, request_id=shared_id)],
    )
    assert result["no_data"] is False
    counts = (result["beacon_count"], result["pageview_count"], result["interaction_count"], result["error_count"])
    assert counts == (3, 3, 1, 1) and result["vitals"]["lcp"]["p75"] == 4.0
    trends = result["trends"]
    assert trends["lcp"] == [4.0, None, None] and trends["cls"] == [0.1, None, 0.2]
    assert trends["pageviews"] == [2, 0, 1] and trends["interactions"] == trends["errors"] == [1, 0, 0]
    assert trends["error_rate"] == [33.33, None, 0.0]
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
    assert (result["errors"][1]["line"], result["errors"][1]["col"]) == (7, 1)

    german = run_analytics(vitals, errors, filters={"country": {"mode": "include", "values": ["DE"]}})
    assert german["environments"]["browsers"] == {"Firefox": 1} and (
        german["error_count"],
        german["pageview_count"],
    ) == (2, 1)

    not_chrome = run_analytics(vitals, errors, filters={"browser": {"mode": "exclude", "values": ["Chrome"]}})
    assert not_chrome["environments"]["browsers"] == {"Firefox": 1} and not_chrome["error_count"] == 2


@_skip_unless_real_ch
def test_real_sql_live_events_carry_real_environment_fields(run_analytics_and_live) -> None:
    ts = "2026-09-01T00:30:00Z"
    env_kw = {
        "browser": "Firefox",
        "os": "Linux",
        "device": "Desktop",
        "city": "Berlin",
        "region": "BE",
        "pop": "FRA",
        "tls": "1.3",
        "ttfb": 0.042,
        "country": "DE",
    }
    events = run_analytics_and_live(
        [_ch_vital(ts, "LCP", 2400, **env_kw)], [_ch_error("2026-09-01T00:29:00Z", error_message="boom")]
    )
    assert [event["type"] for event in events] == ["pageview", "error"]
    first = events[0]
    assert first["browser"] == "Firefox" and first["os"] == "Linux"
    assert (first["raw_log"]["city"], first["raw_log"]["pop"], first["raw_log"]["ttfb"]) == ("Berlin", "FRA", 42)
    assert first["desc"] == "Metric LCP: 2400.0 (GOOD)"
    assert events[1]["browser"] == "Unknown" and events[1]["desc"] == "boom"


@_skip_unless_real_ch
@pytest.mark.parametrize("hours,points", [(48, 49), (49, 3), (24 * 30, 31)])
def test_real_sql_daily_buckets_aggregate_all_hours(run_analytics, hours: int, points: int) -> None:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    result = run_analytics(
        [_ch_vital("2026-09-01T00:00:00Z", "lcp", 1000), _ch_vital("2026-09-01T23:00:00Z", "lcp", 5000)],
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
    assert result["no_data"] is False and result["beacon_count"] == result["error_count"] == 1
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
    vitals = [
        _ch_vital("2026-09-01T00:14:59.999Z", "LCP", 9999),
        _ch_vital("2026-09-01T00:15:00Z", "LCP", 2000),
        _ch_vital("2026-09-01T01:15:00Z", "LCP", 3000),
        _ch_vital("2026-09-01T01:15:00.001Z", "LCP", 9999),
    ]
    result = run_analytics(vitals, [], "2026-08-31T18:15:00-06:00", "2026-08-31T19:15:00-06:00")
    assert result["trends"]["timestamps"] == ["2026-09-01T00:00:00+00:00", "2026-09-01T01:00:00+00:00"]
    assert result["trends"]["lcp"] == [2.0, 3.0] and result["beacon_count"] == 2


@_skip_unless_real_ch
def test_real_sql_metric_formats_zero_and_case_normalization(run_analytics) -> None:
    ts = "2026-09-01T00:00:00Z"
    shared = str(uuid4())
    pairs = [("LCP", 1000), ("lcp", 5000), ("CLS", 0), ("INP", 123.9), ("FID", 12.34), ("FCP", 1500), ("TTFB", 0.5)]
    result = run_analytics([_ch_vital(ts, m, v, request_id=shared) for m, v in pairs], [])
    vitals = result["vitals"]
    assert vitals["lcp"]["p75"] == result["trends"]["lcp"][0] == 4.0
    assert vitals["cls"]["p75"] == result["trends"]["cls"][0] == 0.0
    assert vitals["inp"]["p75"] == 123 and vitals["fid"] == {"p75": 12.3, "fcp": 1.5, "ttfb": 0.5}
    assert vitals["lcp"]["distribution"] == {"good": 100, "needs_improvement": 0, "poor": 0}
    assert result["beacon_count"] == result["pageview_count"] == 1


@_skip_unless_real_ch
def test_real_sql_interactions_only_and_empty_request_ids(run_analytics) -> None:
    res_inter = run_analytics([_ch_vital("2026-09-01T01:00:00Z", "event_click", 0)], [])
    assert res_inter["beacon_count"] == res_inter["interaction_count"] == 1
    assert res_inter["pageview_count"] == res_inter["error_count"] == 0
    assert res_inter["trends"]["lcp"] == res_inter["trends"]["cls"] == [None, None, None]
    assert res_inter["trends"]["error_rate"] == [None, 0.0, None]

    ts = "2026-09-01T00:00:00Z"
    res_empty_req = run_analytics([_ch_vital(ts, "LCP", 1000), _ch_vital(ts, "CLS", 0.1)], [_ch_error(ts)])
    assert res_empty_req["beacon_count"] == res_empty_req["pageview_count"] == res_empty_req["error_count"] == 1
    assert res_empty_req["worst_pages"][0]["error_rate"] == 50.0


# ── Late RUM discovery & reinspection ──
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
    response: dict[str, Any] = {"Contents": [{"Key": key, "ETag": '"etag"', "Size": 1}] if key is not None else []}
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
            "svc", "rum_vitals", page_size=25, cursor=cursor,
            reinspection_cursor=reinspection_cursor, forward_pages_since_reinspection=forward_pages,
        )  # fmt: skip
        cursor = page.next_cursor
        reinspection_cursor = page.next_reinspection_cursor or reinspection_cursor
        forward_pages = page.forward_pages_since_reinspection or 0
    assert (cursor, reinspection_cursor, forward_pages) == (
        "forward-10",
        INITIAL_SOURCE_CURSOR,
        RUM_REINSPECTION_INTERVAL,
    )

    page = lister.list_source_objects(
        "svc", "rum_vitals", page_size=25, cursor=cursor,
        reinspection_cursor=reinspection_cursor, reinspect=True, forward_pages_since_reinspection=forward_pages,
    )  # fmt: skip
    assert page.next_cursor == cursor
    assert page.next_reinspection_cursor == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-1.gz"
    assert page.forward_pages_since_reinspection == 0
    assert client.paginator.requests[-1]["Prefix"] == "raw/rum/"
    assert client.paginator.requests[-1]["PaginationConfig"] == {"PageSize": 25}

    c2 = _DiscoveryClient([_disc_page(key="raw/rum/late-arrival.gz")])
    fwd_cur = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/z.gz"
    p2 = S3SourceObjectLister(c2, bucket="archive-bucket").list_source_objects(
        "svc", "rum_errors", page_size=25, cursor=fwd_cur, reinspection_cursor=INITIAL_SOURCE_CURSOR,
        reinspect=True, forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
    )  # fmt: skip
    assert [o.object_key for o in p2.objects] == ["raw/rum/late-arrival.gz"]
    assert (p2.next_cursor, p2.next_reinspection_cursor, p2.forward_pages_since_reinspection) == (
        fwd_cur,
        INITIAL_SOURCE_CURSOR,
        0,
    )

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


def _make_coord_mocks(src_cursor: str, reinspect_state: tuple[str, int], page: SourceObjectPage, epoch: int = 3):
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(
        current_owner="high_scale", owner_epoch=epoch, source_cursor="owner-cursor"
    )
    control.source_cursor_for.return_value = src_cursor
    control.source_reinspection_state_for.return_value = reinspect_state
    control.claim_sources_batch.return_value = ()
    control.expired_claims.return_value = ()
    lister = MagicMock()
    lister.list_source_objects.return_value = page
    return control, lister


def _run_coord_page(lister: Any, control: Any, domain: str, reader: Any = None, controller: Any = None) -> Any:
    return HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader or MagicMock(),
        controller=controller or MagicMock(),
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="w1",
        control_plane=control,
    ).run_page(service_id="svc", domain=domain)


def test_coordinator_and_postgres_reinspection_cursor_persistence() -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    page1 = SourceObjectPage(
        (),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    control, lister = _make_coord_mocks(forward_cursor, (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL), page1)
    run = _run_coord_page(lister, control, "rum_vitals")
    assert run.next_cursor == forward_cursor
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_called_once_with(
        "svc", "rum_vitals", INITIAL_SOURCE_CURSOR, 0, expected_owner="high_scale", expected_owner_epoch=3
    )

    page2 = SourceObjectPage(
        (), "forward-page-2", next_reinspection_cursor="reinspection-page-2", forward_pages_since_reinspection=3
    )
    control2, lister2 = _make_coord_mocks("forward-page-1", ("reinspection-page-2", 2), page2)
    _run_coord_page(lister2, control2, "rum_errors")
    control2.advance_source_cursor.assert_called_once_with(
        "svc", "forward-page-2", expected_owner="high_scale", expected_owner_epoch=3,
        domain="rum_errors", reinspection_cursor="reinspection-page-2", forward_pages_since_reinspection=3,
    )  # fmt: skip

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
    owner_keys = (
        "service_id",
        "owner_epoch",
        "current_owner",
        "previous_owner",
        "source_cursor",
        "drain_complete",
        "cutover_committed",
        "rollback_allowed",
    )
    owner.fetchone.return_value = dict(
        zip(owner_keys, ("svc", 3, "high_scale", None, "owner-cursor", False, False, False), strict=True)
    )
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
    page = SourceObjectPage(
        (descriptor,),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    control, lister = _make_coord_mocks(
        forward_cursor, (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL), page, epoch=5
    )
    control.discover_source.return_value = SimpleNamespace(status="discovered")
    control.claim_sources_batch.return_value = (SimpleNamespace(object_key=descriptor.object_key),)
    reader, controller = MagicMock(), MagicMock()
    if stage == "read":
        reader.read_source_object.side_effect = RuntimeError("temporary FOS read failure")
    elif stage == "publication":
        reader.read_source_object.return_value = b"source payload"
        controller.ingest_batch.side_effect = RuntimeError("temporary publication failure")
    else:
        reader.read_source_object.side_effect = SourceObjectMissingError("source disappeared")

    run = _run_coord_page(lister, control, "rum_vitals", reader=reader, controller=controller)

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
            "svc", latest, expected_owner="high_scale", expected_owner_epoch=owner.owner_epoch,
            domain="rum_vitals", reinspection_cursor=INITIAL_SOURCE_CURSOR, forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
        )  # fmt: skip
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


# ── Merged from tests/core/test_rum_core.py ──

import gzip
import io
import shutil
import sqlite3
import time
from contextlib import contextmanager

import duckdb
import pytest
from tests._helpers.ledger import (
    DuckLakeLedgerEnv,
    attacher,
    clear_ledger,
    download_stub,
    lake_reader,
    seed_discovered,
)

import backend.core.metadata.ingest_log as quarantine
from backend.core import ingest as ingest_mod
from backend.core.fastly.client import (
    RUM_ASSET_FETCH_NAME,
    RUM_DELIVER_SET_COOKIE_NAME,
    RUM_FARO_FETCH_NAME,
    RUM_RECV_NAME,
    RUM_SIGV4_SIGN_NAME,
    _assert_faro_version_safe,
    generate_rum_asset_fetch_vcl,
    generate_rum_vcl,
    validate_rum_custom_condition,
)
from backend.core.ingest import (
    LEDGER_RECLAIM_AFTER_S,
    _parse_rum_beacon_file,
    _parse_rum_line,
    _quarantine_rum_corrupt_lines,
    convert_batch_rum_objects,
    discover_rum_prefix,
    sweep_rum_ledger_once,
)
from backend.core.log_fields import lint_vcl
from backend.core.metadata.ingest_log import list_quarantine_evidence, read_evidence
from backend.core.rum_ingest import ingest_rum_logs

SERVICE_ID = "test-rum-ledger-edges"
BUCKET = "test-bucket"
PARSE_SVC = "svc-parse-variants"
FALCO_INSTALLED = shutil.which("falco") is not None
VITALS_LINE = json.dumps({"timestamp": "2026-08-27T11:00:00Z", "rum_metric_name": "LCP", "rum_metric_value": "1500"})
ERROR_LINE = json.dumps({"timestamp": "2026-08-27T11:00:01Z", "rum_error_message": "boom", "rum_error_line": "9"})


def _write_gz(path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def _seed_discovered(con, object_key: str, service_id: str = SERVICE_ID) -> None:
    seed_discovered(con, con.cursor(), service_id, [object_key])


def _ledger_row(con, object_key: str, service_id: str = SERVICE_ID):
    return (
        con.cursor()
        .execute(
            "SELECT * FROM ingest_ledger WHERE service_id = ? AND object_key = ?",
            (service_id, object_key),
        )
        .fetchone()
    )


@contextmanager
def _rum_env(tmp_path, download, fos=None, attach=None, service_id: str = SERVICE_ID):
    with DuckLakeLedgerEnv(
        service_id, tmp_path, download, bucket=BUCKET, fos=fos, attach=attach, table_name=None
    ) as real_attach:
        yield real_attach


def _lake_counts(attach) -> tuple[int, int]:
    con = lake_reader(attach)
    try:
        v = (
            con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0]
            if _table_exists(con, "client_vitals")
            else 0
        )
        e = (
            con.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0]
            if _table_exists(con, "client_errors")
            else 0
        )
        return v, e
    finally:
        con.close()


def _table_exists(con, name: str) -> bool:
    return bool(
        con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name='lake' AND table_name=? LIMIT 1", (name,)
        ).fetchone()
    )


def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


_DEFAULT = object()


@contextmanager
def _discover_or_sweep_env(
    service_id: str = SERVICE_ID,
    source: dict | None | object = _DEFAULT,
    list_fn=_empty_list_fos,
    queue_depths=({"q.ingest": 0}, True),
    config: dict | None | object = _DEFAULT,
):
    cfg = {"service_id": service_id} if config is _DEFAULT else config
    src = {"name": "test", "bucket": BUCKET} if source is _DEFAULT else source
    q_patch = (
        patch("backend.celery_app.celery_queue_depths", side_effect=queue_depths)
        if isinstance(queue_depths, Exception)
        else patch("backend.celery_app.celery_queue_depths", return_value=queue_depths)
    )
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest.list_fos_files", side_effect=list_fn) as mock_list,
        q_patch,
        patch("backend.core.ingest.convert_batch_rum_files.delay") as mock_rum_delay,
        patch("backend.core.ingest.convert_batch_files.delay") as mock_batch_delay,
    ):
        yield SimpleNamespace(list_fos=mock_list, rum_delay=mock_rum_delay, batch_delay=mock_batch_delay)


# ── Section 1: RUM Ingest (standard mode) ──


@pytest.fixture
def mock_metadata_db(tmp_path):
    con = sqlite3.connect(str(tmp_path / "test_service.metadata.db"))
    con.row_factory = sqlite3.Row
    con.executescript("""
        CREATE TABLE IF NOT EXISTS ingested_files (
            file_name TEXT, source_name TEXT, ingested_at TEXT DEFAULT (datetime('now')),
            row_count INTEGER, file_size_bytes INTEGER, error_count INTEGER DEFAULT 0,
            file_date DATE, table_name TEXT NOT NULL DEFAULT 'logs',
            PRIMARY KEY (file_name, source_name, table_name)
        );
        CREATE TABLE IF NOT EXISTS ingest_in_flight (
            buffer_filename TEXT NOT NULL, source_name TEXT NOT NULL, files_json TEXT NOT NULL,
            started_at TEXT DEFAULT (datetime('now')), table_name TEXT NOT NULL DEFAULT 'logs',
            PRIMARY KEY (buffer_filename, table_name)
        );
        CREATE TABLE IF NOT EXISTS ingested_files_summary (
            source_name TEXT PRIMARY KEY, file_count INTEGER NOT NULL DEFAULT 0,
            total_rows INTEGER NOT NULL DEFAULT 0, total_bytes INTEGER NOT NULL DEFAULT 0,
            count_with_bytes INTEGER NOT NULL DEFAULT 0, latest_file_name TEXT, last_ingested TEXT
        );
        CREATE TABLE IF NOT EXISTS cron_runs (
            id INTEGER PRIMARY KEY, service_id TEXT, job_name TEXT, duration_seconds REAL,
            status TEXT, rows_ingested INTEGER, error_message TEXT, started_at TEXT
        );
        CREATE TABLE IF NOT EXISTS quarantine_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_id TEXT NOT NULL, source_type TEXT NOT NULL, original_key TEXT NOT NULL,
            line_ordinal INTEGER, byte_offset INTEGER, byte_length INTEGER NOT NULL,
            error_category TEXT NOT NULL, error_text TEXT NOT NULL, sha256 TEXT NOT NULL,
            file_path TEXT NOT NULL DEFAULT '', quarantined_at TEXT DEFAULT (datetime('now'))
        );
    """)
    yield con
    con.close()


@pytest.fixture
def rum_ingest_env(mock_metadata_db):
    with (
        patch("backend.core.rum_ingest.get_source_for_service") as mock_get_source,
        patch("backend.core.rum_ingest._get_fos_client") as mock_get_fos,
        patch("backend.core.metadata.get_con", return_value=mock_metadata_db),
        patch("backend.core.metadata.ingest_log.get_con", return_value=mock_metadata_db),
        patch("backend.core.rum_ingest.start_cron_run", return_value=123) as mock_start,
        patch("backend.core.rum_ingest.log_cron_run") as mock_log,
        patch("backend.core.rum_ingest.finalize_cron_run_if_running") as mock_finalize,
        patch("backend.core.storage.write_to_buffer") as mock_write_buffer,
    ):
        mock_s3 = MagicMock()
        mock_get_fos.return_value = mock_s3
        mock_get_source.return_value = {"name": "test_service", "service_id": "test_service", "bucket": BUCKET}

        def stage_single_object(key: str, content: bytes) -> None:
            mock_s3.get_paginator.return_value.paginate.return_value = [
                {"Contents": [{"Key": key, "Size": len(content)}]}
            ]
            mock_s3.get_object.return_value = {"Body": io.BytesIO(content)}

        yield SimpleNamespace(
            db=mock_metadata_db,
            s3=mock_s3,
            get_source=mock_get_source,
            get_fos=mock_get_fos,
            start=mock_start,
            log=mock_log,
            finalize=mock_finalize,
            write_buffer=mock_write_buffer,
            stage_single_object=stage_single_object,
        )


def test_ingest_rum_logs_empty_bucket(rum_ingest_env):
    rum_ingest_env.get_source.return_value["prefix"] = "test-prefix"
    rum_ingest_env.s3.get_paginator.return_value.paginate.return_value = [{}]
    events = list(ingest_rum_logs("test_service"))
    assert events == [("started", 123), ("done", 0)]
    assert rum_ingest_env.log.call_args[0][3] == "success"
    assert rum_ingest_env.log.call_args[1]["rows_ingested"] == 0
    rum_ingest_env.finalize.assert_called_once_with("test_service", "rum_sync", 123)


@pytest.mark.parametrize(
    "source",
    [None, {"name": "test_service_missing_bucket", "service_id": "test_service_missing_bucket", "bucket": ""}],
)
def test_ingest_rum_logs_skips_when_source_or_bucket_is_missing(source):
    with (
        patch("backend.core.rum_ingest.get_source_for_service", return_value=source),
        patch("backend.core.rum_ingest.start_cron_run", return_value=123),
        patch("backend.core.rum_ingest.log_cron_run") as log_run,
        patch("backend.core.rum_ingest._get_fos_client") as get_fos,
    ):
        events = list(ingest_rum_logs("test_service_missing_bucket"))
    assert events == [("started", 123), ("done", 0)]
    assert log_run.call_args.args[3] == "success"
    assert log_run.call_args.kwargs["rows_ingested"] == 0
    get_fos.assert_not_called()


def test_ingest_rum_logs_records_discovery_failure_and_finalizes_run():
    service_id = "test_service_list_failure"
    with (
        patch(
            "backend.core.rum_ingest.get_source_for_service",
            return_value={"name": service_id, "service_id": service_id, "bucket": BUCKET},
        ),
        patch("backend.core.rum_ingest._get_fos_client", return_value=MagicMock()),
        patch("backend.core.ingest._recover_in_flight"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest.list_fos_files", side_effect=RuntimeError("listing unavailable")),
        patch("backend.core.rum_ingest.start_cron_run", return_value=123),
        patch("backend.core.rum_ingest.log_cron_run") as log_run,
        patch("backend.core.rum_ingest.finalize_cron_run_if_running") as finalize,
    ):
        events = list(ingest_rum_logs(service_id))
    assert events == [("started", 123), ("error", "sync", "listing unavailable")]
    assert log_run.call_args.args[3] == "error"
    assert log_run.call_args.kwargs["error_message"] == "listing unavailable"
    finalize.assert_called_once_with(service_id, "rum_sync", 123)


@pytest.mark.parametrize("capture_failure", [False, True])
def test_ingest_rum_logs_success(rum_ingest_env, capture_failure):
    raw_log = {
        "rum_cid": "sess_123",
        "fastly_req_id": "req_vital_0",
        "rum_metric_name": "LCP",
        "rum_metric_value": 1800.0,
        "rum_metric_rating": "good",
        "rum_pathname": "/about",
        "timestamp": "2026-08-07T03:07:47+00:00",
    }
    malformed_line = b'{"timestamp":\n'
    rum_ingest_env.stage_single_object(
        "raw/rum/rum_log_0.json.gz",
        gzip.compress(json.dumps(raw_log).encode("utf-8") + b"\n" + malformed_line),
    )

    if capture_failure:
        with patch("backend.core.ingest._quarantine_rum_corrupt_lines", side_effect=OSError("evidence unavailable")):
            events = list(ingest_rum_logs("test_service"))
        capture = None
    else:
        with patch(
            "backend.core.metadata.ingest_log.capture_evidence", return_value={"id": 1, "cap_evictions": 0}
        ) as capture:
            events = list(ingest_rum_logs("test_service"))

    assert events == [("started", 123), ("file_done", "rum_log_0.json.gz", 1), ("done", 1)]
    assert rum_ingest_env.write_buffer.call_args[1]["table_name"] == "client_vitals"
    ingested = {r["table_name"]: r["row_count"] for r in rum_ingest_env.db.execute("SELECT * FROM ingested_files")}
    assert ingested == {"client_vitals": 1, "client_errors": 0}
    assert rum_ingest_env.log.call_args.kwargs["outcome_counters"] == {
        "valid_records": 1,
        "malformed_records": 1,
        "corrupt_containers": 0,
        "quarantine_capture_failures": int(capture_failure),
        "source_delete_failures": 0,
        "cap_evictions": 0,
        "objects_processed": 1,
        "objects_successful": 0,
        "objects_partial": int(not capture_failure),
        "objects_failed": int(capture_failure),
    }
    assert rum_ingest_env.log.call_args.args[3] == "error"
    if not capture_failure:
        assert capture.call_args.args[3] == malformed_line


def test_ingest_rum_logs_preserves_query_metrics_and_exception_context(rum_ingest_env):
    rows = [
        {
            "timestamp": "2026-08-07T03:07:47+00:00",
            "url": "https://example.com/rum?rum_metric_name=LCP&rum_metric_value=1234.5"
            "&rum_metric_rating=good&cid=client-1&rum_pathname=%2Fcheckout",
            "fastly_req_id": "request-vital",
        },
        {
            "timestamp": "2026-08-07T03:07:48+00:00",
            "referer": "https://example.com/deep/path?ignored=1",
            "rum_error_message": "Uncaught ReferenceError",
            "rum_error_file": "checkout.js",
            "rum_error_line": "7",
            "fastly_req_id": "request-error",
        },
    ]
    rum_ingest_env.stage_single_object(
        "raw/rum/query-and-error.json.gz",
        gzip.compress(b"\n".join(json.dumps(row).encode() for row in rows) + b"\n"),
    )
    events = list(ingest_rum_logs("test_service"))
    assert events == [("started", 123), ("file_done", "query-and-error.json.gz", 2), ("done", 2)]
    written = {c.kwargs["table_name"]: c.args[1].to_pylist() for c in rum_ingest_env.write_buffer.call_args_list}
    vital, error = written["client_vitals"][0], written["client_errors"][0]
    assert (vital["metric_name"], vital["metric_value"], vital["cid"], vital["pathname"]) == (
        "LCP",
        1234.5,
        "client-1",
        "/checkout",
    )
    assert (error["error_message"], error["error_file"], error["error_line"], error["pathname"]) == (
        "Uncaught ReferenceError",
        "checkout.js",
        7,
        "/deep/path",
    )


def test_ingest_rum_logs_retains_whole_corrupt_container_and_fails_its_run(rum_ingest_env, monkeypatch, tmp_path):
    service_id = "rum-corrupt-gzip"
    broken_gzip = b"\x1f\x8bnot a valid gzip stream"
    monkeypatch.setattr("backend.config.SERVICES_DATA_DIR", tmp_path)
    rum_ingest_env.start.return_value = 124
    rum_ingest_env.get_source.return_value = {"name": service_id, "service_id": service_id, "bucket": BUCKET}
    rum_ingest_env.stage_single_object("raw/rum/corrupt.json.gz", broken_gzip)

    events = list(ingest_rum_logs(service_id))
    assert (
        events[0] == ("started", 124)
        and events[1][:2] == ("error", "s3://test-bucket/raw/rum/corrupt.json.gz")
        and events[-1] == ("done", 0)
    )
    assert rum_ingest_env.log.call_args.args[3] == "error"
    assert rum_ingest_env.log.call_args.kwargs["outcome_counters"]["corrupt_containers"] == 1
    evidence = list_quarantine_evidence(service_id)
    item, evidence_file = quarantine.read_evidence(service_id, evidence[0]["id"])
    with evidence_file:
        assert evidence_file.read() == broken_gzip
    assert item["original_key"] == "raw/rum/corrupt.json.gz"


def test_ingest_rum_logs_faro_dual_fan_out(rum_ingest_env):
    faro_payload = {
        "measurements": [{"type": "web-vitals", "values": {"FID": 45.0}, "meta": {"rating": "good"}}],
        "exceptions": [
            {
                "value": "Uncaught ReferenceError: x is not defined",
                "stacktrace": {"frames": [{"filename": "bundle.js", "lineno": 100, "colno": 2}]},
            }
        ],
    }
    raw_log = {
        "rum_body": json.dumps(faro_payload),
        "fastly_req_id": "req_faro_dual",
        "rum_cid": "faro_sess",
        "timestamp": "2026-08-07T03:07:47+00:00",
    }
    rum_ingest_env.stage_single_object("raw/rum/rum_log_1.json.gz", gzip.compress(json.dumps(raw_log).encode("utf-8")))
    events = list(ingest_rum_logs("test_service"))
    assert events == [("started", 123), ("file_done", "rum_log_1.json.gz", 2), ("done", 2)]
    ingested = {r["table_name"]: r["row_count"] for r in rum_ingest_env.db.execute("SELECT * FROM ingested_files")}
    assert ingested == {"client_vitals": 1, "client_errors": 1}


def test_ingest_rum_logs_list_fos_files_error_marks_run_error(rum_ingest_env):
    rum_ingest_env.get_source.return_value["prefix"] = "test-prefix"

    def fake_list_gen(**kwargs):
        yield {"type": "error", "message": "FOS list failed: Access Denied"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    with patch("backend.core.ingest.list_fos_files", side_effect=fake_list_gen):
        events = list(ingest_rum_logs("test_service"))
    assert any(evt[0] == "error" for evt in events)
    assert rum_ingest_env.log.call_args[0][3] == "error"


def test_ingest_rum_logs_incremental_idle_tick_bounds_list_calls(rum_ingest_env):
    for table_name in ("client_vitals", "client_errors"):
        rum_ingest_env.db.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name) VALUES (?, ?, 10, 100, ?)",
            (
                "s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/file1.json.gz",
                "test_service",
                table_name,
            ),
        )
    rum_ingest_env.db.commit()
    list_calls = []

    def fake_paginate(**kwargs):
        list_calls.append(kwargs)
        if kwargs.get("Prefix") == "raw/rum/":
            return [
                {
                    "Contents": [
                        {
                            "Key": f"raw/rum/year=2026/month=10/day=07/hour=10/minute={i:02d}/old_{j}.json.gz",
                            "Size": 100,
                        }
                        for j in range(100)
                    ]
                }
                for i in range(10)
            ]
        return [{"Contents": []}]

    rum_ingest_env.s3.get_paginator.return_value.paginate.side_effect = fake_paginate
    assert list(ingest_rum_logs("test_service")) == [("started", 123), ("done", 0)]
    assert len(list_calls) <= 5 and all("minute=" in c.get("Prefix", "") for c in list_calls)


def test_ingest_rum_logs_time_budget_allows_first_chunk_trap_41(rum_ingest_env, monkeypatch):
    fake_files = [
        f"s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/f_{i}.json.gz" for i in range(100)
    ]
    gz_content = gzip.compress(VITALS_LINE.encode("utf-8"))
    rum_ingest_env.s3.get_object.return_value = {"Body": io.BytesIO(gz_content)}

    def fake_list_gen(**kwargs):
        if False:
            yield {}
        return {
            "new_files": fake_files,
            "file_sizes": {f: len(gz_content) for f in fake_files},
            "skipped_already": 0,
            "stranded_already": [],
        }

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)
    current_time, download_count = 1000.0, 0
    monkeypatch.setattr(time, "time", lambda: current_time)
    orig_download = ingest_mod._download_chunk_to_local

    def counting_download(s3_client, chunk, tmpdir):
        nonlocal current_time, download_count
        download_count += 1
        res = orig_download(s3_client, chunk, tmpdir)
        current_time += 100.0
        return res

    monkeypatch.setattr(ingest_mod, "_download_chunk_to_local", counting_download)
    list(ingest_rum_logs("test_service", max_seconds=20))
    assert download_count == 1


def test_ingest_rum_logs_deletes_raw_objects_inline_and_excludes_failures(rum_ingest_env, monkeypatch):
    file_good = "s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/good.json.gz"
    file_bad = "s3://test-bucket/raw/rum/year=2026/month=10/day=07/hour=14/minute=50/bad.json.gz"
    content_good = gzip.compress(VITALS_LINE.encode() + b"\n")
    content_bad = gzip.compress(b'{"timestamp":\n')
    deleted_batches = []

    monkeypatch.setattr(
        "backend.core.ingest._delete_objects_robust_with_failures",
        lambda _f, _b, keys: (deleted_batches.append(list(keys)) or len(keys), set()),
    )

    def fake_list_gen(**kwargs):
        if False:
            yield {}
        return {
            "new_files": [file_good, file_bad],
            "file_sizes": {file_good: len(content_good), file_bad: len(content_bad)},
            "skipped_already": 0,
            "stranded_already": [],
        }

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)
    rum_ingest_env.s3.get_object.side_effect = lambda **kw: {
        "Body": io.BytesIO(content_good if "good.json.gz" in kw.get("Key", "") else content_bad)
    }
    with patch("backend.core.ingest._quarantine_rum_corrupt_lines", side_effect=OSError("quarantine unavailable")):
        list(ingest_rum_logs("test_service"))

    all_deleted = [k for batch in deleted_batches for k in batch]
    assert "raw/rum/year=2026/month=10/day=07/hour=14/minute=50/good.json.gz" in all_deleted
    assert "raw/rum/year=2026/month=10/day=07/hour=14/minute=50/bad.json.gz" not in all_deleted


# ── Section 2: RUM Ledger Discovery, Sweep, and Convert ──


def test_discover_rum_prefix_defaults_to_rum_raw_and_inserts_ledger_rows():
    service_id = "test-celery-rum-svc"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=05/beacons.json.gz"
    _, cur = clear_ledger(service_id)
    captured_kwargs = {}

    def mock_list(*args, **kwargs):
        captured_kwargs.update(kwargs)
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://{BUCKET}/{object_key}"],
            "file_sizes": {f"s3://{BUCKET}/{object_key}": 999},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with _discover_or_sweep_env(service_id, list_fn=mock_list) as env:
        assert discover_rum_prefix(service_id, run_id=813) == 1
        env.rum_delay.assert_called_once_with(service_id, [object_key])

    assert captured_kwargs["prefix_subpath"] == "raw/rum/"
    row = cur.execute(
        "SELECT status, size_bytes, originating_task, originating_run_id FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert (row["status"], row["size_bytes"], row["originating_task"], row["originating_run_id"]) == (
        "discovered",
        999,
        "rum_discovery",
        813,
    )


def test_discover_rum_prefix_honors_explicit_minute_subpath():
    service_id = "test-celery-rum-svc"
    clear_ledger(service_id)
    captured_kwargs = {}

    def mock_list(*args, **kwargs):
        captured_kwargs.update(kwargs)
        yield from _empty_list_fos()
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    minute_prefix = "raw/rum/year=2026/month=08/day=27/hour=10/minute=05/"
    with _discover_or_sweep_env(service_id, list_fn=mock_list):
        discover_rum_prefix(service_id, prefix_subpath=minute_prefix)
    assert captured_kwargs["prefix_subpath"] == minute_prefix


def test_discover_rum_prefix_noops_and_deduplicates():
    con, _ = clear_ledger(SERVICE_ID)
    with _discover_or_sweep_env(config=None) as env:
        assert discover_rum_prefix(SERVICE_ID) == 0
        env.list_fos.assert_not_called()

    with (
        patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list,
    ):
        assert discover_rum_prefix(SERVICE_ID) == 0
        mock_list.assert_not_called()

    def _null_list(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return None

    with _discover_or_sweep_env(list_fn=_null_list) as env:
        assert discover_rum_prefix(SERVICE_ID) == 0
        env.rum_delay.assert_not_called()

    object_key = "raw/rum/2026/08/27/11/10/known.json.gz"
    _seed_discovered(con, object_key)

    def _list_one(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        p = f"s3://{BUCKET}/{object_key}"
        return {"new_files": [p], "file_sizes": {p: 10}, "skipped_already": 0, "stranded_already": []}

    with _discover_or_sweep_env(list_fn=_list_one) as env:
        assert discover_rum_prefix(SERVICE_ID) == 0
        env.rum_delay.assert_not_called()


def test_rum_ledger_sweep_persists_its_originating_run():
    service_id = "test-celery-rum-sweep-origin"
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=06/beacons.json.gz"
    con, _ = clear_ledger(service_id)

    def list_one(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://{BUCKET}/{object_key}"],
            "file_sizes": {f"s3://{BUCKET}/{object_key}": 777},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with _discover_or_sweep_env(service_id, list_fn=list_one):
        result = sweep_rum_ledger_once(service_id, run_id=91)
    assert result["discovered"] == 1
    row = _ledger_row(con, object_key, service_id)
    assert (row["originating_task"], row["originating_run_id"]) == ("ledger_rum_sweep", 91)


def test_sweep_rum_ledger_reclaim_and_scoping():
    service_id = "test-celery-rum-svc"
    con, cur = clear_ledger(service_id)
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    rum_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=10/stuck.json.gz"
    regular_key = "raw/request/year=2026/month=08/day=27/hour=10/minute=10/stuck.json.gz"
    for k in (rum_key, regular_key):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) VALUES (?, ?, 'claimed', 'w1', ?, ?)",
            (service_id, k, stale, stale - 30),
        )
    con.commit()

    with _discover_or_sweep_env(service_id) as env:
        summary = sweep_rum_ledger_once(service_id)
        env.rum_delay.assert_called_once_with(service_id, [rum_key])
        env.batch_delay.assert_not_called()

    assert summary["reclaimed"] == 1
    assert _ledger_row(con, rum_key, service_id)["status"] == "discovered"
    assert _ledger_row(con, regular_key, service_id)["status"] == "claimed"


def test_sweep_rum_ledger_scopes_to_source_prefix():
    con, cur = clear_ledger(SERVICE_ID)
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    prefixed = "tenant-a/raw/rum/2026/08/27/11/12/mine.json.gz"
    unprefixed = "raw/rum/2026/08/27/11/12/not-mine.json.gz"
    for key in (prefixed, unprefixed):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) VALUES (?, ?, 'claimed', 'dead-worker', ?, ?)",
            (SERVICE_ID, key, stale, stale - 30),
        )
    con.commit()

    with _discover_or_sweep_env(source={"name": "t", "bucket": BUCKET, "prefix": "/tenant-a/"}) as env:
        summary = sweep_rum_ledger_once(SERVICE_ID)
    assert summary["reclaimed"] == 1
    env.rum_delay.assert_called_once_with(SERVICE_ID, [prefixed])
    assert _ledger_row(con, unprefixed)["status"] == "claimed"


@pytest.mark.parametrize(
    "case,queue_depths,next_attempt_offset",
    [
        ("queue_backlog", ({"q.ingest": 50}, True), 0),
        ("retry_not_due", ({"q.ingest": 0}, True), 3600),
        ("broker_down", Exception("broker unreachable"), 0),
    ],
)
def test_sweep_rum_ledger_skips_redispatch_conditions(case, queue_depths, next_attempt_offset):
    con, cur = clear_ledger(SERVICE_ID)
    now = time.time()
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, next_attempt_at) VALUES (?, ?, 'discovered', ?, ?)",
        (SERVICE_ID, f"raw/rum/{case}.gz", now - 3600, now + next_attempt_offset if next_attempt_offset else None),
    )
    con.commit()

    with _discover_or_sweep_env(queue_depths=queue_depths) as env:
        summary = sweep_rum_ledger_once(SERVICE_ID)
    assert summary["redispatched"] == 0
    env.rum_delay.assert_not_called()
    if case == "broker_down":
        assert summary["broker_ok"] is False


@pytest.mark.parametrize("status", ["committed", "quarantined", "dead_letter"])
def test_sweep_rum_ledger_leaves_terminal_rows_alone(status):
    con, cur = clear_ledger(SERVICE_ID)
    key = f"raw/rum/2026/08/27/11/13/{status}.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_at, discovered_at) VALUES (?, ?, ?, ?, ?)",
        (SERVICE_ID, key, status, time.time() - 999999, time.time() - 999999),
    )
    con.commit()

    with _discover_or_sweep_env(queue_depths=({}, False)) as env:
        summary = sweep_rum_ledger_once(SERVICE_ID)
    assert (summary["reclaimed"], summary["redispatched"], summary["discovered"]) == (0, 0, 0)
    env.rum_delay.assert_not_called()
    assert _ledger_row(con, key)["status"] == status


def test_convert_rum_object_splits_and_idempotent_redelivery(tmp_path):
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=05/beacons.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    raw_file = tmp_path / "beacons.json"
    _write_gz(raw_file, [VITALS_LINE, ERROR_LINE])

    for _ in range(2):
        con.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?) "
            "ON CONFLICT (service_id, object_key) DO UPDATE SET status='discovered'",
            (SERVICE_ID, object_key, time.time()),
        )
        con.commit()
        with _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw_file)})) as attach:
            summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "test-worker")
        assert summary["committed"] == 1

    assert _lake_counts(attach) == (1, 1)
    row = _ledger_row(con, object_key)
    assert row["status"] == "committed" and row["committed_at"] is not None


def test_convert_rum_object_quarantines_malformed_line_and_commits_valid_rows(tmp_path):
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=07/mixed.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)
    bad_line = '{"timestamp": "2026-08-27T10:07:01Z", "rum_metric_name": "broken"'
    raw_file = tmp_path / "mixed.json"
    _write_gz(raw_file, [VITALS_LINE, bad_line])
    fos_mock = MagicMock()

    with (
        _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw_file)}), fos=fos_mock) as attach,
        patch(
            "backend.core.metadata.ingest_log.capture_evidence",
            return_value={"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0},
        ) as capture,
    ):
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "test-worker")

    assert summary["committed"] == 1
    assert _lake_counts(attach) == (1, 0)
    fos_mock.put_object.assert_not_called()
    capture.assert_called_once()
    assert capture.call_args.args == (SERVICE_ID, "rum", object_key, (bad_line + "\n").encode())
    assert capture.call_args.kwargs["line_ordinal"] == 2


def test_convert_rum_object_partial_write_failure_does_not_mark_committed(tmp_path):
    object_key = "raw/rum/year=2026/month=08/day=27/hour=10/minute=08/partial.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)
    broken_error = json.dumps(
        {"timestamp": "2026-08-27T10:08:01Z", "rum_error_message": "boom", "rum_error_line": "99999999999999"}
    )
    raw_file = tmp_path / "partial.json"
    _write_gz(raw_file, [VITALS_LINE, broken_error])

    with _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw_file)})) as attach:
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "test-worker")
    assert summary["failed"] == 1
    row = _ledger_row(con, object_key)
    assert (row["status"], row["committed_at"], row["attempts"]) == ("discovered", None, 1)
    assert _lake_counts(attach) == (1, 0)

    _write_gz(raw_file, [VITALS_LINE, ERROR_LINE])
    with _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw_file)})) as attach:
        summary2 = convert_batch_rum_objects(SERVICE_ID, [object_key], "test-worker")
    assert summary2["committed"] == 1
    assert _lake_counts(attach) == (1, 1)


def test_convert_rum_object_returns_not_claimed_when_already_committed(tmp_path):
    object_key = "raw/rum/2026/08/27/11/00/gone.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at) VALUES (?, ?, 'committed', ?, ?)",
        (SERVICE_ID, object_key, time.time(), time.time()),
    )
    con.commit()
    raw = tmp_path / "gone.json"
    _write_gz(raw, [VITALS_LINE])
    with _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})) as attach:
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
    assert (summary["not_claimed"], summary["committed"]) == (1, 0)
    assert _lake_counts(attach) == (0, 0)


def test_convert_rum_object_uses_ducklake_detach_in_finally(tmp_path):
    object_key = "raw/rum/2026/08/27/11/02/detach_test.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)
    raw = tmp_path / "detach_test.json"
    _write_gz(raw, [VITALS_LINE])
    with (
        _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})),
        patch("backend.core.storage._core._ducklake_detach") as mock_detach,
    ):
        assert convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")["committed"] == 1
        assert mock_detach.called


def test_convert_batch_rum_objects_separates_deleted_and_transient_downloads(tmp_path):
    object_keys = [
        "raw/rum/2026/08/27/11/03/already-deleted.json.gz",
        "raw/rum/2026/08/27/11/04/still-live.json.gz",
    ]
    con, _ = clear_ledger(SERVICE_ID)
    for k in object_keys:
        _seed_discovered(con, k)

    def _head_object_404_first(*, Bucket, Key):
        if Key == object_keys[0]:
            raise RuntimeError("404 Not Found")
        return {"ContentLength": 123}

    fos = MagicMock()
    fos.head_object.side_effect = _head_object_404_first
    with _rum_env(tmp_path, download_stub({}), fos=fos):
        summary = convert_batch_rum_objects(SERVICE_ID, object_keys, "w1")

    assert (summary["dead_letter"], summary["failed"]) == (1, 1)
    dead = _ledger_row(con, object_keys[0])
    assert dead["status"] == "dead_letter" and dead["attempts"] == 0 and "missing from FOS" in dead["last_error"]
    transient = _ledger_row(con, object_keys[1])
    assert transient["status"] == "discovered" and transient["attempts"] == 1 and "transient" in transient["last_error"]


@pytest.mark.parametrize("failure_mode", ["quarantine_return_fail", "quarantine_raises", "bookkeeping_raises"])
def test_convert_rum_object_commits_valid_rows_despite_side_effect_failures(tmp_path, failure_mode):
    object_key = f"raw/rum/2026/08/27/11/05/{failure_mode}.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)
    raw = tmp_path / f"{failure_mode}.json"
    _write_gz(raw, [VITALS_LINE, ERROR_LINE, '{"timestamp": "2026-08-27T11:05:00Z", "rum_metric_name": "trunc'])

    ctx = (
        patch(
            "backend.core.metadata.ingest_log.capture_evidence",
            return_value={"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1},
        )
        if failure_mode == "quarantine_return_fail"
        else patch("backend.core.ingest._quarantine_rum_corrupt_lines", side_effect=OSError("evidence disk full"))
        if failure_mode == "quarantine_raises"
        else patch("backend.core.ingest.metadata_db.insert_ingested_files", side_effect=Exception("db locked"))
    )
    with _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})) as attach, ctx:
        summary = convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")
    assert summary["committed"] == 1
    assert _ledger_row(con, object_key)["status"] == "committed"
    assert _lake_counts(attach) == (1, 1)


def test_convert_rum_object_backfills_source_file_column_on_v2_table(tmp_path):
    object_key = "raw/rum/2026/08/27/11/07/widen.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)
    attach = attacher(str(tmp_path / "cat.ducklake"), str(tmp_path / "lakedata"))
    setup = duckdb.connect()
    attach(setup, None)
    setup.execute(
        "CREATE TABLE lake.client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR, "
        "pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, "
        "city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE)"
    )
    setup.execute(
        "INSERT INTO lake.client_vitals (timestamp, metric_name, metric_value) VALUES ('2026-08-01T00:00:00Z', 'v2-legacy-row', 1.0)"
    )
    setup.close()

    raw = tmp_path / "widen.json"
    _write_gz(raw, [VITALS_LINE])
    with _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}), attach=attach):
        assert convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")["committed"] == 1

    reader = lake_reader(attach)
    rows = reader.execute("SELECT metric_name, _source_file FROM lake.client_vitals ORDER BY metric_name").fetchall()
    reader.close()
    assert rows == [("LCP", f"s3://{BUCKET}/{object_key}"), ("v2-legacy-row", None)]


def test_convert_rum_object_rolls_back_on_schema_drifted_table_or_failed_attach(tmp_path):
    object_key = "raw/rum/2026/08/27/11/08/drift.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)
    attach = attacher(str(tmp_path / "cat.ducklake"), str(tmp_path / "lakedata"))
    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.client_vitals (timestamp TIMESTAMPTZ, metric_name VARCHAR, _source_file VARCHAR)")
    setup.close()

    raw = tmp_path / "drift.json"
    _write_gz(raw, [VITALS_LINE, ERROR_LINE])
    with _rum_env(tmp_path, download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}), attach=attach):
        assert convert_batch_rum_objects(SERVICE_ID, [object_key], "w1")["failed"] == 1
    row = _ledger_row(con, object_key)
    assert (row["status"], row["committed_at"], row["attempts"]) == ("discovered", None, 1)

    attach_key = "raw/rum/2026/08/27/11/05/attach-failed.json.gz"
    _seed_discovered(con, attach_key)
    with _rum_env(
        tmp_path,
        download_stub({f"s3://{BUCKET}/{attach_key}": str(raw)}),
        attach=lambda *_a, **_kw: False,
    ):
        assert convert_batch_rum_objects(SERVICE_ID, [attach_key], "w1")["failed"] == 1
    assert "attach failed" in _ledger_row(con, attach_key)["last_error"]


def test_convert_batch_rum_objects_multi_file_and_edge_cases(tmp_path):
    con, _ = clear_ledger(SERVICE_ID, quarantine=True)
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        assert convert_batch_rum_objects(SERVICE_ID, [], "w")["requested"] == 0
        assert convert_batch_rum_objects(SERVICE_ID, ["raw/rum/unknown.json.gz"], "w")["not_claimed"] == 1
        no_src_key = "raw/rum/2026/08/27/11/02/no-source.json.gz"
        _seed_discovered(con, no_src_key)
        assert convert_batch_rum_objects(SERVICE_ID, [no_src_key], "w")["failed"] == 1
        assert "no source registered" in _ledger_row(con, no_src_key)["last_error"]

    clear_ledger(SERVICE_ID, quarantine=True)
    object_keys = ["raw/rum/2026/08/27/11/00/batch-a.json.gz", "raw/rum/2026/08/27/11/01/batch-b.json.gz"]
    local_files = {}
    for idx, key in enumerate(object_keys):
        _seed_discovered(con, key)
        lf = tmp_path / f"batch-{idx}.json.gz"
        _write_gz(lf, [VITALS_LINE, ERROR_LINE])
        if idx == 0:
            with gzip.open(lf, "ab") as raw:
                raw.write(b'{"invalid":\x00}\n')
        local_files[f"s3://{BUCKET}/{key}"] = str(lf)

    with _rum_env(tmp_path, download_stub(local_files)) as attach:
        summary = convert_batch_rum_objects(SERVICE_ID, object_keys, "worker-batch-rum")
    assert summary["committed"] == 2
    assert _lake_counts(attach) == (2, 2)
    evidence_rows = list_quarantine_evidence(SERVICE_ID)
    assert len(evidence_rows) == 1 and evidence_rows[0]["original_key"] == object_keys[0]
    _, ev_file = read_evidence(SERVICE_ID, int(evidence_rows[0]["id"]))
    with ev_file:
        assert ev_file.read() == b'{"invalid":\x00}\n'


def test_quarantine_rum_corrupt_lines_captures_and_validates_prefix(tmp_path):
    fos = MagicMock()
    src = {"service_id": SERVICE_ID, "name": "prod-source", "bucket": BUCKET, "prefix": "/tenant-a/"}
    object_key = "tenant-a/raw/rum/2026/08/27/11/09/mixed.json.gz"
    bad = [b"{bad one\n", b'{"a":1}\n']
    local_file = tmp_path / "mixed.json"
    with gzip.open(local_file, "wb") as raw:
        raw.write(b"".join(bad))
    corrupt = [("{bad one", "invalid_json"), ('{"a":1}', "parse_error: nope")]

    with patch(
        "backend.core.metadata.ingest_log.capture_evidence",
        return_value={"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0},
    ) as capture:
        result = _quarantine_rum_corrupt_lines(fos, src, object_key, corrupt, valid_count=7, local_file=str(local_file))
    assert result == {"valid_records": 7, "malformed_records": 2, "cap_evictions": 0, "quarantine_capture_failures": 0}
    assert capture.call_count == 2

    res_bad_prefix = _quarantine_rum_corrupt_lines(
        fos,
        {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""},
        "raw/2026/08/27/regular-log.json.gz",
        [("x", "invalid_json")],
        1,
        local_file=str(local_file),
    )
    assert res_bad_prefix["quarantine_capture_failures"] == 1


# ── Section 3: RUM Beacon Line & File Parsing Variants ──


def _faro_payload() -> dict:
    return {
        "meta": {"browser": {"name": "Firefox", "mobile": True}, "os": {"name": "Android"}},
        "page": {"url": "https://shop.example.com/checkout/cart?step=2"},
        "measurements": [
            {"type": "web-vitals", "context": {"rating": "needs-improvement"}, "values": {"lcp": 2600.5, "delta": 12.0}}
        ],
        "exceptions": [
            {
                "value": "TypeError: undefined is not a function",
                "stacktrace": {"frames": [{"filename": "bundle.js", "lineno": 918, "colno": 27}]},
            }
        ],
    }


@pytest.mark.parametrize("double_encoded", [False, True])
def test_parse_rum_line_extracts_faro_body(double_encoded):
    body = json.dumps(json.dumps(_faro_payload())) if double_encoded else json.dumps(_faro_payload())
    log_data = {
        "timestamp": "2026-08-27T10:00:00Z",
        "rum_body": body,
        "rum_cid": "cid-abc",
        "fastly_req_id": "req-123",
        "geo_city": "Portland",
        "geo_country_code": "US",
        "server_pop": "PDX",
        "tls_version": "TLSv1.3",
        "time_to_first_byte": "88.5",
    }
    vitals_rows, errors_rows = _parse_rum_line(log_data, PARSE_SVC)
    assert [r["metric_name"] for r in vitals_rows] == ["lcp"]
    v = vitals_rows[0]
    assert (v["metric_value"], v["metric_rating"], v["pathname"], v["browser"], v["os"], v["device"]) == (
        2600.5,
        "needs-improvement",
        "/checkout/cart",
        "Firefox",
        "Android",
        "Mobile",
    )
    assert (v["city"], v["country"], v["pop"], v["tls"], v["ttfb"]) == ("Portland", "US", "PDX", "TLSv1.3", 88.5)
    assert len(errors_rows) == 1 and errors_rows[0]["error_file"] == "bundle.js" and errors_rows[0]["error_line"] == 918


@pytest.mark.parametrize(
    "log_data,expected_vital",
    [
        (
            {
                "timestamp": "2026-08-27T10:00:00Z",
                "rum_body": "{not json",
                "rum_metric_name": "CLS",
                "rum_metric_value": "0.08",
                "rum_metric_rating": "good",
                "rum_pathname": "/pricing",
            },
            {"metric_name": "CLS", "metric_value": 0.08, "metric_rating": "good", "pathname": "/pricing"},
        ),
        (
            {
                "timestamp": "2026-08-27T10:00:00Z",
                "rum_body": json.dumps({"meta": {}, "page": {"url": "https://example.com/blog/post-1"}}),
            },
            {
                "metric_name": "pageview",
                "metric_value": 1.0,
                "pathname": "/blog/post-1",
                "device": "Desktop",
                "browser": "Chrome",
                "os": "macOS",
            },
        ),
        (
            {
                "timestamp": "2026-08-27T10:01:00Z",
                "url": "/rum-beacon?rum_metric_name=INP&rum_metric_value=145.25&rum_metric_rating=good&cid=cid-from-query&rum_pathname=/search",
            },
            {
                "metric_name": "INP",
                "metric_value": 145.25,
                "metric_rating": "good",
                "cid": "cid-from-query",
                "pathname": "/search",
            },
        ),
        (
            {
                "timestamp": "2026-08-27T10:01:00Z",
                "rum_metric_name": "LCP",
                "rum_metric_value": "1200",
                "rum_metric_rating": "good",
                "rum_cid": "cid-explicit",
                "rum_pathname": "/explicit",
                "url": "/rum-beacon?rum_metric_name=INP&rum_metric_value=999&rum_metric_rating=poor&cid=cid-query&rum_pathname=/from-query",
            },
            {
                "metric_name": "LCP",
                "metric_value": 1200,
                "metric_rating": "good",
                "cid": "cid-explicit",
                "pathname": "/explicit",
            },
        ),
        (
            {"timestamp": "2026-08-27T10:01:00Z", "rum_raw_query": "/b?rum_metric_name=TTFB&rum_metric_value=60.5"},
            {"metric_name": "TTFB", "metric_value": 60.5},
        ),
        (
            {
                "timestamp": "2026-08-27T10:01:00Z",
                "rum_metric_name": "LCP",
                "rum_metric_value": "not-a-number",
                "rum_pathname": "/broken",
            },
            {"metric_name": "LCP", "metric_value": 0.0, "pathname": "/broken"},
        ),
        (
            {
                "timestamp": "2026-08-27T10:01:00Z",
                "rum_metric_name": "FCP",
                "rum_metric_value": "700",
                "referer": "https://shop.example.com/collections/shoes?page=3",
            },
            {"pathname": "/collections/shoes"},
        ),
        (
            {"timestamp": "2026-08-27T10:01:00Z", "rum_metric_name": "FCP", "rum_metric_value": "700"},
            {"pathname": "/"},
        ),
        (
            {
                "timestamp": "2026-08-27T10:01:00Z",
                "rum_metric_name": "FCP",
                "rum_metric_value": "700",
                "rum_pathname": "//collections//shoes",
            },
            {"pathname": "/collections/shoes"},
        ),
    ],
)
def test_parse_rum_line_vitals_variants(log_data, expected_vital):
    vitals_rows, errors_rows = _parse_rum_line(log_data, PARSE_SVC)
    assert errors_rows == [] and len(vitals_rows) == 1
    for k, v in expected_vital.items():
        assert vitals_rows[0][k] == v


def test_parse_rum_line_error_tenancy_and_timestamps():
    vitals_rows, errors_rows = _parse_rum_line(
        {
            "timestamp": "2026-08-27T10:02:00Z",
            "rum_error_message": "ReferenceError: gtag is not defined",
            "rum_error_file": "analytics.js",
            "rum_error_line": "17",
            "rum_error_col": "5",
            "rum_pathname": "/account",
        },
        PARSE_SVC,
    )
    assert vitals_rows == [] and len(errors_rows) == 1
    assert (errors_rows[0]["error_file"], errors_rows[0]["error_line"], errors_rows[0]["error_col"]) == (
        "analytics.js",
        17,
        5,
    )

    assert _parse_rum_line({"rum_service_id": "other", "rum_metric_name": "LCP"}, PARSE_SVC) is None
    assert (
        _parse_rum_line(
            {"service_id": PARSE_SVC, "timestamp": "2026-08-27T10:00:00Z", "rum_metric_name": "LCP"}, PARSE_SVC
        )
        is not None
    )

    with pytest.raises(ValueError, match="missing_timestamp"):
        _parse_rum_line({"rum_metric_name": "LCP", "rum_metric_value": "1"}, PARSE_SVC)
    with pytest.raises(ValueError, match="invalid_timestamp"):
        _parse_rum_line({"timestamp": "not-a-timestamp", "rum_metric_name": "LCP", "rum_metric_value": "1"}, PARSE_SVC)

    naive_vitals, _ = _parse_rum_line(
        {"timestamp": "2026-08-27T10:03:00", "rum_metric_name": "LCP", "rum_metric_value": "1"}, PARSE_SVC
    )
    assert naive_vitals[0]["timestamp"].utcoffset().total_seconds() == 0


def test_parse_rum_beacon_file_variants(tmp_path):
    f1 = tmp_path / "mixed.json.gz"
    _write_gz(
        f1,
        [
            VITALS_LINE,
            ERROR_LINE,
            '{"timestamp": "2026-08-27T10:05:02Z", "rum_metric_name": "CLS"',
            json.dumps({"timestamp": "2026-08-27T10:00:00Z", "service_id": "svc-other", "rum_metric_name": "LCP"}),
            "",
            "   ",
            "[1, 2, 3]",
            json.dumps({"rum_metric_name": "LCP", "rum_metric_value": "1.0"}),
            json.dumps({"timestamp": "bad-iso", "rum_metric_name": "INP", "rum_metric_value": "150"}),
        ],
    )
    vitals, errors, corrupt = _parse_rum_beacon_file(str(f1), PARSE_SVC)
    assert len(vitals) == 1 and len(errors) == 1 and len(corrupt) == 4
    reasons = [r for _, r in corrupt]
    assert "invalid_json" in reasons
    assert any(r.startswith("parse_error: ") for r in reasons)
    assert any("missing_timestamp" in r for r in reasons)
    assert any("invalid_timestamp" in r for r in reasons)


# ── Section 4: RUM VCL Provisioning & Falco Validation ──


def test_generate_rum_vcl_and_asset_fetch_without_faro():
    v_default = generate_rum_vcl("srv_test")
    assert v_default == generate_rum_vcl("srv_test", faro_version=None)
    assert set(v_default.keys()) == {RUM_RECV_NAME, RUM_DELIVER_SET_COOKIE_NAME}
    assert "faro" not in "\n".join(v_default.values()).lower()
    assert "faro" not in "\n".join(generate_rum_vcl("srv_test", faro_version="2.9.0").values()).lower()

    no_faro = generate_rum_asset_fetch_vcl("iad-va-us")
    assert no_faro == generate_rum_asset_fetch_vcl("iad-va-us", faro_version=None)
    assert set(no_faro.keys()) == {RUM_ASSET_FETCH_NAME, RUM_SIGV4_SIGN_NAME}
    assert "faro" not in "\n".join(no_faro.values()).lower()


def test_faro_asset_fetch_routing_caching_and_shield_pop():
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")
    assert set(snippets.keys()) == {RUM_ASSET_FETCH_NAME, RUM_SIGV4_SIGN_NAME, RUM_FARO_FETCH_NAME}

    recv, sigv4, fetch = snippets[RUM_ASSET_FETCH_NAME], snippets[RUM_SIGV4_SIGN_NAME], snippets[RUM_FARO_FETCH_NAME]
    assert 'if ((req.url.path == "/js/rum.js" || req.url.path == "/js/faro-sdk.js") && req.method == "GET")' in recv
    assert recv.count("return(lookup);") == 1
    assert 'bereq.url.path == "/js/faro-sdk.js"' in sigv4
    assert 'set bereq.url = "/rum/faro-web-sdk-v2.9.0.iife.js";' in sigv4
    assert 'set bereq.url = "/rum/rum-tracker.js";' in sigv4

    assert fetch.index("if (beresp.status == 200)") < fetch.index(
        'set beresp.http.Surrogate-Key = "rum-faro-sdk rum-js";'
    )
    assert "set beresp.cacheable = false;" in fetch
    assert "immutable" not in fetch
    assert 'set beresp.http.Surrogate-Control = "max-age=604800";' in fetch
    assert int(re.search(r'Cache-Control = "[^"]*max-age=(\d+)', fetch).group(1)) < 604800

    assert (
        "fastly.try_select_shield(ssl_shield_frankfurt_de, F_fos_origin)"
        in generate_rum_asset_fetch_vcl("frankfurt-de", faro_version="1.0.0")[RUM_ASSET_FETCH_NAME]
    )
    assert (
        "set req.backend = F_fos_origin;"
        in generate_rum_asset_fetch_vcl("none", faro_version="1.0.0")[RUM_ASSET_FETCH_NAME]
    )


@pytest.mark.parametrize(
    "bad_version",
    [
        '1.2."3',
        "1.2.3\n",
        "../../etc/passwd",
        "1.2.3 ",
        " 1.2.3",
        "1.2.3; set req.http.x = 1",
        "1.2",
        "1.2.3.4",
        "1.2.x",
        "",
        "1.2.3-beta",
        "1.2\\.3",
        "1.2.3\x00",
        "\x00",
        "１.２.３",
        "١.٢.٣",
        "๑.๒.๓",
        "2.9.٠",
        '2.9.0"; unset bereq.http.Authorization; #',
    ],
)
def test_faro_version_rejects_malicious_or_non_ascii_versions(bad_version):
    with pytest.raises(ValueError):
        _assert_faro_version_safe(bad_version)
    with pytest.raises(ValueError):
        generate_rum_asset_fetch_vcl("iad-va-us", faro_version=bad_version)
    with pytest.raises(ValueError):
        generate_rum_vcl("srv_test", faro_version=bad_version)


def _wrap_for_falco(body: str, subroutine: str) -> str:
    stage = subroutine.removeprefix("vcl_")
    return (
        'backend F_fos_origin { .host = "example.com"; .port = "443"; .ssl = true; }\n'
        'backend ssl_shield_iad_va_us { .host = "example.com"; .port = "443"; .ssl = true; }\n'
        'table fos_credentials { "access_key": "x", }\n'
        f"sub {subroutine} {{\n  #FASTLY {stage}\n{body}\n}}\n"
    )


@pytest.mark.skipif(not FALCO_INSTALLED, reason="requires falco binary")
@pytest.mark.parametrize(
    "faro_version,snip_id,subroutine",
    [
        ("2.9.0", RUM_ASSET_FETCH_NAME, "vcl_recv"),
        ("2.9.0", RUM_SIGV4_SIGN_NAME, "vcl_miss"),
        ("2.9.0", RUM_FARO_FETCH_NAME, "vcl_fetch"),
        (None, RUM_ASSET_FETCH_NAME, "vcl_recv"),
        (None, RUM_SIGV4_SIGN_NAME, "vcl_miss"),
    ],
)
def test_falco_lints_rum_asset_fetch_snippets(faro_version, snip_id, subroutine):
    snippets = generate_rum_asset_fetch_vcl("iad-va-us", faro_version=faro_version)
    result = lint_vcl(_wrap_for_falco(snippets[snip_id], subroutine), snippet_name=snip_id, wrap_subroutine=None)
    assert result.ok, f"falco errors: {result.errors}"


def test_falco_missing_binary_and_custom_conditions(monkeypatch):
    monkeypatch.setattr("backend.core.log_fields._falco_binary", lambda: None)
    res = lint_vcl("if (true) {}", wrap_subroutine=None)
    assert res.skipped is True and res.ok is True

    for cond in (
        "",
        "   ",
        None,
        'req.http.User-Agent !~ "(bot|crawler)"',
        'client.geo.country_code != "US"',
        'req.http.Host == "example.com"',
    ):
        assert validate_rum_custom_condition(cond) is None

    for bad_path in (
        'req.url.path !~ "^/api/"',
        'req.url.path ~ "^/api/"',
        'req.url ~ "^/api/"',
        'client.ip != "203.0.113.5" && req.url.path !~ "^/api/"',
    ):
        err = validate_rum_custom_condition(bad_path)
        assert err is not None and "req.url" in err and "tracker" in err

    single_q = validate_rum_custom_condition("client.ip != '192.168.1.1'")
    assert single_q is not None and "double quotes" in single_q


# ═════════════════════════════════════════════════════════════════════════════
# 4. Faro Versions (merged from tests/core/test_faro_versions.py)
# ═════════════════════════════════════════════════════════════════════════════

import base64
import os
import tarfile

from backend.core import faro_versions
from backend.core.faro_versions import (
    _BUNDLE_MEMBER_NAME,
    _is_stable_numeric,
    fetch_available_faro_versions,
    fetch_faro_bundle,
)

TARBALL_URL = "https://registry.npmjs.org/@grafana/faro-web-sdk/-/faro-web-sdk-2.9.0.tgz"

SAMPLE_REGISTRY_RESPONSE = {
    "name": "@grafana/faro-web-sdk",
    "versions": {
        "1.9.0": {"version": "1.9.0"},
        "2.10.0": {"version": "2.10.0"},
        "2.9.0": {"version": "2.9.0"},
        "1.4.5": {"version": "1.4.5"},
        "2.8.2": {"version": "2.8.2"},
        "2.9.0-alpha.1": {"version": "2.9.0-alpha.1"},
        "2.9.0+build.7": {"version": "2.9.0+build.7"},
    },
}

SAMPLE_BUNDLE = b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"


def _add_tar_member(tar: tarfile.TarFile, name: str, content: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(content)
    tar.addfile(info, io.BytesIO(content))


def _build_fixture_tarball(
    *,
    bundle_name: str = _BUNDLE_MEMBER_NAME,
    bundle_content: bytes = SAMPLE_BUNDLE,
    include_bundle: bool = True,
    extra_files: dict[str, bytes] | None = None,
) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        _add_tar_member(tar, "package/package.json", b'{"name": "@grafana/faro-web-sdk", "version": "2.9.0"}')
        _add_tar_member(tar, "package/README.md", b"# Faro Web SDK\n\nMuch longer than the bundle itself.\n" * 20)
        _add_tar_member(tar, "package/dist/bundle/faro-web-sdk.iife.js.map", b'{"version":3,"sources":[]}')
        if include_bundle:
            _add_tar_member(tar, bundle_name, bundle_content)
        for name, content in (extra_files or {}).items():
            _add_tar_member(tar, name, content)
    return buf.getvalue()


SAMPLE_TARBALL = _build_fixture_tarball()


def _sri_integrity(data: bytes, algo: str = "sha512") -> str:
    digest = hashlib.new(algo, data).digest()
    return f"{algo}-{base64.b64encode(digest).decode()}"


def _registry_payload_with_dist(version_dist: dict[str, dict]) -> dict:
    return {
        "name": "@grafana/faro-web-sdk",
        "versions": {v: {"version": v, "dist": dist} for v, dist in version_dist.items()},
    }


def _route_by_url(*, registry: httpx.Response | None = None, responses: dict[str, httpx.Response] | None = None):
    responses = dict(responses or {})

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == faro_versions.REGISTRY_URL:
            assert registry is not None, "unexpected registry metadata request"
            return registry
        assert url in responses, f"unexpected request to {url}"
        return responses[url]

    return handler


_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _mock_transport(handler):
    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_ASYNC_CLIENT(*args, **kwargs)

    return factory


@pytest.fixture
def mock_http(monkeypatch):
    def install(handler):
        monkeypatch.setattr(faro_versions.httpx, "AsyncClient", _mock_transport(handler))

    return install


@pytest.mark.parametrize("version", ["１.２.３", "١.٢.٣", "2.9.٠"])
def test_is_stable_numeric_rejects_non_ascii_digits(version):
    assert _is_stable_numeric(version) is False


def test_is_stable_numeric_accepts_plain_ascii_version():
    assert _is_stable_numeric("2.9.0") is True


async def test_fetch_available_faro_versions_excludes_non_ascii_digit_keys(mock_http):
    payload = {
        "name": "@grafana/faro-web-sdk",
        "versions": {
            "2.9.0": {"version": "2.9.0"},
            "١.٢.٣": {"version": "١.٢.٣"},
            "１.２.３": {"version": "１.２.３"},
        },
    }
    mock_http(lambda _: httpx.Response(200, content=json.dumps(payload).encode()))
    assert await fetch_available_faro_versions() == ["2.9.0"]


async def test_fetch_available_faro_versions_filters_and_sorts_descending(mock_http):
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == faro_versions.REGISTRY_URL
        return httpx.Response(200, content=json.dumps(SAMPLE_REGISTRY_RESPONSE).encode())

    mock_http(handler)
    versions = await fetch_available_faro_versions()
    assert versions == ["2.10.0", "2.9.0", "2.8.2", "1.9.0", "1.4.5"]
    assert all("-" not in v and "+" not in v for v in versions)


def _raise_connect_error(req: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("registry unreachable", request=req)


@pytest.mark.parametrize(
    "handler,match",
    [
        (lambda _: httpx.Response(500, content=b"upstream error"), "500"),
        (
            _raise_connect_error,
            "Failed to fetch Faro versions",
        ),
        (lambda _: httpx.Response(200, content=b"<html>proxy interstitial</html>"), "malformed registry JSON"),
        (lambda _: httpx.Response(200, content=json.dumps({"name": "x"}).encode()), "no 'versions' map"),
    ],
)
async def test_fetch_available_faro_versions_errors(mock_http, handler, match):
    mock_http(handler)
    with pytest.raises(ValueError, match=match):
        await fetch_available_faro_versions()


async def test_fetch_faro_bundle_returns_extracted_bundle_bytes(mock_http):
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=SAMPLE_TARBALL)},
        )
    )
    bundle = await fetch_faro_bundle("2.9.0")
    assert isinstance(bundle, bytes) and bundle == SAMPLE_BUNDLE and b"Faro" in bundle


def test_extracted_bundle_bytes_differ_from_tarball_bytes():
    assert SAMPLE_BUNDLE != SAMPLE_TARBALL
    assert SAMPLE_BUNDLE not in SAMPLE_TARBALL or len(SAMPLE_BUNDLE) < len(SAMPLE_TARBALL)
    assert len(SAMPLE_TARBALL) > len(SAMPLE_BUNDLE)


async def test_fetch_faro_bundle_verifies_against_shasum_fallback(mock_http):
    registry_payload = _registry_payload_with_dist(
        {
            "2.9.0": {
                "shasum": hashlib.sha1(SAMPLE_TARBALL, usedforsecurity=False).hexdigest(),
                "tarball": TARBALL_URL,
            }
        }
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=SAMPLE_TARBALL)},
        )
    )
    assert await fetch_faro_bundle("2.9.0") == SAMPLE_BUNDLE


async def test_fetch_faro_bundle_raises_on_tarball_hash_mismatch(mock_http):
    mutated_tarball = _build_fixture_tarball(bundle_content=b"!function(e){/* poisoned */}(window);")
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=mutated_tarball)},
        )
    )
    with pytest.raises(ValueError, match="failed integrity verification"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_member_missing_from_verified_archive(mock_http):
    tarball_without_bundle = _build_fixture_tarball(include_bundle=False)
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(tarball_without_bundle), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=tarball_without_bundle)},
        )
    )
    with pytest.raises(ValueError, match="missing expected member"):
        await fetch_faro_bundle("2.9.0")


@pytest.mark.parametrize("malicious_name", ["/etc/passwd", "../../../etc/passwd", "package/../../../etc/passwd"])
async def test_fetch_faro_bundle_rejects_malicious_member_name(mock_http, malicious_name):
    hostile_tarball = _build_fixture_tarball(
        include_bundle=False,
        extra_files={malicious_name: b"attacker-controlled bytes"},
    )
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(hostile_tarball), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=hostile_tarball)},
        )
    )
    with pytest.raises(ValueError, match="missing expected member"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_registry_has_no_dist_for_version(mock_http):
    registry_payload = _registry_payload_with_dist(
        {"1.0.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=SAMPLE_TARBALL)},
        )
    )
    with pytest.raises(ValueError, match="no dist metadata"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_tarball_url_untrusted_host(mock_http):
    registry_payload = _registry_payload_with_dist(
        {
            "2.9.0": {
                "integrity": _sri_integrity(SAMPLE_TARBALL),
                "tarball": "https://evil.example/faro-web-sdk-2.9.0.tgz",
            }
        }
    )
    mock_http(lambda request: httpx.Response(200, content=json.dumps(registry_payload).encode()))
    with pytest.raises(ValueError, match="untrusted tarball URL host"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_on_404_and_server_error(mock_http):
    registry_404 = _registry_payload_with_dist(
        {"99.99.99": {"integrity": _sri_integrity(b"irrelevant"), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_404).encode()),
            responses={TARBALL_URL: httpx.Response(404, content=b"Cannot find package")},
        )
    )
    with pytest.raises(ValueError, match="404") as exc_info:
        await fetch_faro_bundle("99.99.99")
    assert "99.99.99" in str(exc_info.value)

    registry_503 = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_503).encode()),
            responses={TARBALL_URL: httpx.Response(503, content=b"service unavailable")},
        )
    )
    with pytest.raises(ValueError, match="503") as exc_info_503:
        await fetch_faro_bundle("2.9.0")
    assert "404" not in str(exc_info_503.value)


async def test_fetch_faro_bundle_raises_on_network_errors(mock_http):
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )

    def tarball_fail(request: httpx.Request) -> httpx.Response:
        if str(request.url) == faro_versions.REGISTRY_URL:
            return httpx.Response(200, content=json.dumps(registry_payload).encode())
        raise httpx.ConnectError("tarball host unreachable", request=request)

    mock_http(tarball_fail)
    with pytest.raises(ValueError, match="Failed to download Faro tarball"):
        await fetch_faro_bundle("2.9.0")

    def registry_fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("registry unreachable", request=request)

    mock_http(registry_fail)
    with pytest.raises(ValueError, match="Failed to verify Faro bundle"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_tarball_exceeds_size_ceiling(mock_http):
    oversized = b"0" * (faro_versions._TARBALL_MAX_BYTES + 1)
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(oversized), "tarball": TARBALL_URL}}
    )
    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=oversized)},
        )
    )
    with pytest.raises(ValueError, match="exceeds size ceiling"):
        await fetch_faro_bundle("2.9.0")


@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("FARO_LIVE_REGISTRY_TEST") != "1",
    reason="opt-in live network test against the real npm registry; set FARO_LIVE_REGISTRY_TEST=1 to run",
)
async def test_live_registry_tarball_verifies():
    bundle = await fetch_faro_bundle(faro_versions.DEFAULT_FARO_VERSION)
    assert isinstance(bundle, bytes) and len(bundle) > 1000 and (b"Faro" in bundle or b"faro" in bundle)


@pytest.fixture(autouse=True)
def _reset_versions_cache():
    faro_versions.reset_versions_cache()
    yield
    faro_versions.reset_versions_cache()


async def test_cached_versions_fetches_once_within_ttl(monkeypatch):
    calls = {"n": 0}

    async def _fake():
        calls["n"] += 1
        return ["2.10.0", "2.9.0"]

    monkeypatch.setattr(faro_versions, "fetch_available_faro_versions", _fake)
    first = await faro_versions.get_faro_versions_cached()
    second = await faro_versions.get_faro_versions_cached()
    assert first == ["2.10.0", "2.9.0"] and second == first and calls["n"] == 1


async def test_cached_versions_serves_stale_or_empty_on_error(monkeypatch):
    async def _boom():
        raise ValueError("registry down")

    monkeypatch.setattr(faro_versions, "fetch_available_faro_versions", _boom)
    assert await faro_versions.get_faro_versions_cached() == []

    async def _ok():
        return ["2.10.0", "2.9.0"]

    monkeypatch.setattr(faro_versions, "fetch_available_faro_versions", _ok)
    assert await faro_versions.get_faro_versions_cached() == ["2.10.0", "2.9.0"]

    faro_versions._versions_cache = (0.0, ["2.10.0", "2.9.0"])
    monkeypatch.setattr(faro_versions, "fetch_available_faro_versions", _boom)
    assert await faro_versions.get_faro_versions_cached() == ["2.10.0", "2.9.0"]
