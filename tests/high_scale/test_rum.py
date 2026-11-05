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
    control.claim_sources_batch = MagicMock(return_value=())
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

import pytest

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
from backend.core.log_fields import lint_vcl
from backend.core.rum_ingest import (
    _parse_rum_beacon_file,
    _parse_rum_line,
    _quarantine_rum_corrupt_lines,
)

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


def test_parse_rum_line_expands_batched_querystring_metrics_and_errors():
    batched_url = (
        "/rum-beacon?service_id=svc-parse"
        "&rum_metric_name=FCP&rum_metric_value=450&rum_metric_rating=good"
        "&rum_metric_name=LCP&rum_metric_value=1850.5&rum_metric_rating=good"
        "&rum_metric_name=CLS&rum_metric_value=0.12&rum_metric_rating=needs-improvement"
        "&rum_metric_name=TTFB&rum_metric_value=92.5&rum_metric_rating=good"
        "&rum_metric_name=domComplete&rum_metric_value=2100&rum_metric_rating="
        "&rum_error_message=TypeError%3A+x+is+null&rum_error_file=main.js&rum_error_line=42&rum_error_col=8"
        "&rum_error_message=RangeError%3A+bad&rum_error_file=vendor.js&rum_error_line=10&rum_error_col=2"
        "&cid=cid-batch-1&rum_pathname=%2Fcheckout"
    )
    # VCL querystring.get populates top-level fields with the FIRST occurrence in req.url
    log_data = {
        "timestamp": "2026-08-27T10:05:00Z",
        "rum_metric_name": "FCP",
        "rum_metric_value": "450",
        "rum_metric_rating": "good",
        "rum_error_message": "TypeError: x is null",
        "rum_error_file": "main.js",
        "rum_cid": "cid-batch-1",
        "rum_pathname": "/checkout",
        "url": batched_url,
        "rum_raw_query": batched_url,
        "browser": "Firefox",
        "os": "Linux",
        "device": "Desktop",
    }
    vitals_rows, errors_rows = _parse_rum_line(log_data, PARSE_SVC)
    assert [
        (r["metric_name"], r["metric_value"], r["metric_rating"], r["ttfb"], r["cid"], r["pathname"])
        for r in vitals_rows
    ] == [
        ("FCP", 450.0, "good", 92.5, "cid-batch-1", "/checkout"),
        ("LCP", 1850.5, "good", 92.5, "cid-batch-1", "/checkout"),
        ("CLS", 0.12, "needs-improvement", 92.5, "cid-batch-1", "/checkout"),
        ("TTFB", 92.5, "good", 92.5, "cid-batch-1", "/checkout"),
        ("domComplete", 2100.0, "", 92.5, "cid-batch-1", "/checkout"),
    ]
    assert [
        (e["error_message"], e["error_file"], e["error_line"], e["error_col"], e["cid"], e["pathname"])
        for e in errors_rows
    ] == [
        ("TypeError: x is null", "main.js", 42, 8, "cid-batch-1", "/checkout"),
        ("RangeError: bad", "vendor.js", 10, 2, "cid-batch-1", "/checkout"),
    ]


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


def test_parse_and_convert_zstd_rum_beacon_file(tmp_path):
    import zstandard as zstd

    f_zst = tmp_path / "mixed.log.zst"
    bad_line = '{"timestamp": "2026-08-27T10:05:02Z", "rum_metric_name": "CLS"'
    raw_bytes = ("\n".join([VITALS_LINE, ERROR_LINE, bad_line]) + "\n").encode("utf-8")
    f_zst.write_bytes(zstd.ZstdCompressor().compress(raw_bytes))

    vitals, errors, corrupt = _parse_rum_beacon_file(str(f_zst), SERVICE_ID)
    assert len(vitals) == 1 and len(errors) == 1 and len(corrupt) == 1
    assert corrupt[0][1] == "invalid_json"


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


def test_rum_beacon_health_enforces_15m_recency_and_counts_error_only_beacons() -> None:
    from backend.high_scale.rum import rum_beacon_health

    observed = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    expected_cutoff = observed - timedelta(minutes=15)

    class StaleOnlyClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            self.calls.append((sql, params or {}))
            return [{"c": 5, "last_ts": observed - timedelta(minutes=30)}]

    stale_client = StaleOnlyClient()
    stale_health = rum_beacon_health(service_for(stale_client), now=observed)
    assert stale_health["has_data"] is False
    assert stale_health["beacons"] == 0 and stale_health["recent_beacons"] == 0
    assert stale_health["setup_complete"] is False and stale_health["last_beacon_time"] is None
    agg_sql, agg_params = stale_client.calls[0]
    assert agg_params["cutoff"] == expected_cutoff and agg_params["service_id"] == "TestRum"
    assert "rum_vitals_aggregates" in agg_sql and "rum_error_aggregates" in agg_sql
    assert "bucket_start >= {cutoff:DateTime64(3, 'UTC')}" in agg_sql

    class ErrorOnlyFactFallbackClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            self.calls.append((sql, params or {}))
            if "rum_vitals_aggregates" in sql:
                return [{"c": 0, "last_ts": None}]
            if "rum_error_facts" in sql:
                return [{"c": 4, "last_ts": observed - timedelta(minutes=3)}]
            return []

    err_client = ErrorOnlyFactFallbackClient()
    err_health = rum_beacon_health(service_for(err_client), now=observed)
    assert err_health["has_data"] is True
    assert err_health["beacons"] == 4 and err_health["recent_beacons"] == 4
    assert err_health["setup_complete"] is True
    assert err_health["last_beacon_time"] == "2026-09-01T11:57:00Z"
    facts_sql, facts_params = err_client.calls[1]
    assert "rum_vitals_facts" in facts_sql and "rum_error_facts" in facts_sql
    assert "domain='rum_vitals'" in facts_sql and "domain='rum_errors'" in facts_sql
    assert facts_params["cutoff"] == expected_cutoff


def test_truncated_rum_body_falls_back_to_query_or_top_level_vcl_fields() -> None:
    from backend.high_scale.archive import ArchiveSourceObject
    from backend.high_scale.query import decode_source_object

    src = ArchiveSourceObject(
        service_id="TestRum",
        domain="rum_vitals",
        object_key="2026-09-01/truncated.log",
        checksum="sha256:truncated",
        size_bytes=256,
        version="v1",
    )
    line_qparam = json.dumps(
        {
            "timestamp": "2026-09-01T12:00:00Z",
            "url": "/_fos/b?rum_metric_name=TTFB&rum_metric_value=110&cid=c-qparam",
            "rum_body": '{"meta":{"sdk":',
        }
    )
    line_top_level = json.dumps(
        {
            "timestamp": "2026-09-01T12:00:01Z",
            "url": "/_fos/b",
            "rum_cid": "c-top",
            "rum_metric_name": "TTFB",
            "rum_metric_value": 110,
            "rum_body": '{"meta":{"sdk":',
        }
    )
    decoded_ok = decode_source_object(
        src,
        f"{line_qparam}\n{line_top_level}\n".encode(),
        transform_version="v1",
        domain="rum_vitals",
    )
    assert decoded_ok.accepted_rows == 2
    assert decoded_ok.quarantined_rows == 0
    assert [e["metric_name"] for e in decoded_ok.events] == ["TTFB", "TTFB"]
    assert [e["metric_value"] for e in decoded_ok.events] == [110.0, 110.0]

    line_no_fallback = json.dumps(
        {
            "timestamp": "2026-09-01T12:00:02Z",
            "rum_body": "{not json",
        }
    )
    decoded_dead = decode_source_object(
        src,
        f"{line_no_fallback}\n".encode(),
        transform_version="v1",
        domain="rum_vitals",
    )
    assert decoded_dead.accepted_rows == 0
    assert decoded_dead.quarantined_rows == 1
    assert decoded_dead.dead_letters[0].reason == "invalid Faro RUM body"
