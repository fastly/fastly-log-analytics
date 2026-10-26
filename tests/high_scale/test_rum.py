"""Regression coverage for the native RUM chart producer, fields, filters, environments, and late discovery."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import make_dataclass
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.policy import default
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch
from uuid import uuid4

import boto3
import httpx
import psycopg
import pytest
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject, ServingWatermark
from backend.high_scale.archive_publication import (
    ArchivePublication,
    InMemoryObjectStore,
    write_batch_archive_checkpoint,
)
from backend.high_scale.decoder import decode_source_object
from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.publication import (
    _DOMAIN_COLUMNS,
    ClickHousePublication,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PartialInsert,
    _batch_uuid,
    _event_uuid,
    _rows_for_domain,
    rum_environment,
    rum_error_position,
)
from backend.high_scale.recovery import (
    RecoveryCapacityMeasurement,
    RecoveryReport,
    evaluate_recovery_capacity,
    purge_cmcd_projections,
    purge_origin_projections,
    purge_rum_facts,
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_serving_state_from_fos,
)
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.replay import replay_manifest, replay_performance_projections
from backend.high_scale.rum import parse_rum_filters, rum_analytics, rum_live_events
from backend.high_scale.rum_recovery import RumRecoveryBatchAdapter, _content, execute_rum_recovery, plan_rum_recovery
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
from fastapi import FastAPI
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool
from structlog.testing import capture_logs

from backend import config
from backend.config import ClickHouseConfig
from backend.core import clickhouse_client
from backend.core.request_context import build_request_context
from backend.core.rum_ingest import extract_metrics_from_faro_payload
from backend.high_scale import postgres_control, registry_bootstrap, rum_recovery
from backend.high_scale import publication as pub_mod
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.routers import rum as rum_router
from tests.conftest import override_request_context
from tests.high_scale.conftest import (
    InMemoryManifestCatalog,
    make_test_archive_manifest,
)
from tests.high_scale.conftest import (
    ReplayClickHouseClient as ReplayClient,
)
from tests.high_scale.conftest import (
    event_digest as _event_digest,
)
from tests.high_scale.conftest import (
    make_archive_parquet_bytes as _parquet,
)
from tests.high_scale.conftest import (
    sha256_checksum as _checksum,
)

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


# ── Merged from tests/high_scale/test_recovery.py ──

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
_manifest = partial(
    make_test_archive_manifest, retention_deadline=NOW + timedelta(days=1), deletion_deadline=NOW + timedelta(days=2)
)
_Catalog = partial(InMemoryManifestCatalog, filter_source=False)


def _pub_env(*items, fail_on: set[str] | None = None, client=None):
    archive = ArchivePublication(InMemoryObjectStore())
    manifests = []
    for m, b in items:
        archive.publish(m, b)
        manifests.append(m)
    ch = client or ReplayClient(fail_on=fail_on)
    return _Catalog(tuple(manifests)), archive, ClickHousePublication(InMemoryBatchManifestStore(), ch), ch


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        return []


class _RecordingReplayClient(ReplayClient):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list = []

    def insert(self, batch):
        self.batches.append(batch)
        return super().insert(batch)


def _recent_and_warm():
    return (
        _manifest("recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)),
        _manifest(
            "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
        ),
    )


def test_recovery_prioritizes_recent_history_and_reports_full_completion() -> None:
    catalog, archive, publication, _ = _pub_env(*_recent_and_warm())
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.manifests_replayed == 2 and report.rows_replayed == 2
    assert report.triage_available and report.raw_query_available and report.warm_history_available
    assert report.full_rebuild_complete and report.errors == ()


def test_recovery_reports_partial_availability_when_warm_tier_fails() -> None:
    catalog, archive, publication, _ = _pub_env(*_recent_and_warm(), fail_on={"warm"})
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.triage_available and report.raw_query_available
    assert not report.warm_history_available and not report.full_rebuild_complete
    assert len(report.errors) == 1 and "warm" in report.errors[0]


def test_recovery_reports_no_triage_when_recent_tier_fails() -> None:
    catalog, archive, publication, _ = _pub_env(_recent_and_warm()[0], fail_on={"recent"})
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert not report.triage_available and not report.raw_query_available and not report.full_rebuild_complete


def test_recovery_reinserts_rows_whose_manifest_was_already_replayed_once() -> None:
    rec = _recent_and_warm()[0]
    catalog, archive, publication, client = _pub_env(rec)
    first = replay_manifest(rec[0], archive, publication)
    assert first.publication.duplicate is False and client.insert_calls == ["replay:recent"]

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.rows_replayed == 1 and report.manifests_replayed == 1 and report.full_rebuild_complete
    assert len(client.insert_calls) == 2 and client.insert_calls[0] != client.insert_calls[1]


def test_resuming_a_rebuild_with_the_same_rebuild_id_stays_idempotent() -> None:
    catalog, archive, publication, client = _pub_env(_recent_and_warm()[0])
    first = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert len(client.insert_calls) == 1
    second = rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, rebuild_id=first.rebuild_id
    )
    assert len(client.insert_calls) == 1 and second.manifests_replayed == 1 and second.rebuild_id == first.rebuild_id


def test_recovery_with_no_archived_history_is_a_trivially_complete_rebuild() -> None:
    catalog, archive, publication, _ = _pub_env()
    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert report.manifests_replayed == 0 and report.full_rebuild_complete and report.ingest_accepted


def test_origin_projection_rebuild_is_resumable_without_replaying_request_facts() -> None:
    rec = _manifest(
        "recent-origin",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=59),
        ({"timestamp": (NOW - timedelta(hours=1)).isoformat(), "url": "/slow", "ottfb": 2500, "ost": 503},),
    )
    catalog, archive, publication, client = _pub_env(rec)
    first = rebuild_origin_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    second = rebuild_origin_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 3
    assert client.insert_calls == [
        f"rebuild-origin:{first.rebuild_id}:recent-origin:summary",
        f"rebuild-origin:{first.rebuild_id}:recent-origin:dimensions",
    ]


def _performance_events() -> tuple[dict, ...]:
    return (
        dict(
            event_id="perf-1",
            timestamp=(NOW - timedelta(hours=1)).isoformat(),
            url="/slow",
            cache="MISS",
            elapsed=250_000,
            ttfb=0.1,
            ottfb=40_000,
            ottlb=90_000,
        ),
    )


def test_performance_projection_rebuild_publishes_only_the_new_tables_and_is_resumable() -> None:
    rec = _manifest("recent-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _performance_events())
    catalog, archive, publication, client = _pub_env(rec)
    first = rebuild_performance_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    second = rebuild_performance_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 2 and first.full_rebuild_complete
    assert client.insert_calls == [
        f"rebuild-performance:{first.rebuild_id}:recent-perf:summary",
        f"rebuild-performance:{first.rebuild_id}:recent-perf:scatter",
    ]


def test_performance_projection_rebuild_reports_a_failed_manifest_without_stopping() -> None:
    bad = _manifest("bad-perf", NOW - timedelta(hours=2), NOW - timedelta(hours=1), _performance_events())
    good = _manifest("good-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _performance_events())
    catalog, archive, publication, _ = _pub_env(bad, good, fail_on={"bad-perf"})
    report = rebuild_performance_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    assert report.manifests_replayed == 1 and not report.full_rebuild_complete
    assert len(report.errors) == 1 and "bad-perf" in report.errors[0]


def test_performance_projection_replay_rejects_non_request_manifests() -> None:
    rum = SimpleNamespace(source=SimpleNamespace(domain="rum_vitals"))
    _, archive, publication, _ = _pub_env()
    with pytest.raises(ValueError, match="request manifests"):
        replay_performance_projections(rum, archive, publication, rebuild_id="r")


def test_purge_origin_projections_deletes_only_the_service_window_from_origin_tables() -> None:
    client = _Recorder()
    purge_origin_projections(client, service_id="svc", since=NOW - timedelta(days=30))
    assert [sql.split()[2].strip("`") for sql, _ in client.calls] == [
        "origin_minute_summary",
        "origin_minute_dimensions",
    ]
    for sql, params in client.calls:
        assert sql.startswith("ALTER TABLE") and "mutations_sync=2" in sql
        assert "DELETE WHERE service_id={service_id:String} AND bucket_start >= {since:DateTime64(3)}" in sql
        assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


def _cmcd_events() -> tuple[dict, ...]:
    ts = (NOW - timedelta(hours=1)).isoformat()
    return (
        {
            "event_id": "evt-1",
            "service_id": "svc",
            "timestamp": ts,
            "country": "US",
            "asn": 7922,
            "cmcd": {"sid": "s-1", "br": "1200", "bs": None},
        },
        {
            "event_id": "evt-2",
            "service_id": "svc",
            "timestamp": ts,
            "country": "DE",
            "asn": None,
            "cmcd": {"sid": None, "br": None, "bs": None},
        },
        {"event_id": "evt-3", "service_id": "svc", "timestamp": ts, "country": "FR", "asn": 3215, "cmcd": None},
    )


def test_cmcd_projection_rebuild_publishes_geography_enriched_rows_and_is_resumable() -> None:
    rec = _manifest("recent-cmcd", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _cmcd_events())
    catalog, archive, publication, client = _pub_env(rec, client=_RecordingReplayClient())
    first = rebuild_cmcd_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    second = rebuild_cmcd_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id=first.rebuild_id
    )
    assert first.full_rebuild_complete and first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 1
    assert client.insert_calls == [f"rebuild-cmcd:{first.rebuild_id}:recent-cmcd"]
    (batch,) = client.batches
    assert batch.domain == "cmcd"
    (row,) = batch.rows
    assert row["request_event_id"] == "evt-1" and row["country"] == "US" and row["asn"] == 7922
    assert row["fields"] == {"sid": "s-1", "br": "1200"}


def test_cmcd_projection_rebuild_reports_a_failing_manifest_without_stopping() -> None:
    rec = _manifest("recent-cmcd", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), _cmcd_events())
    catalog, archive, publication, _ = _pub_env(rec, fail_on={"rebuild-cmcd"})
    report = rebuild_cmcd_projections_from_fos(catalog, archive, publication, service_id="svc", now=NOW)
    assert not report.full_rebuild_complete and report.manifests_replayed == 0 and "recent-cmcd" in report.errors[0]


def test_purge_cmcd_projections_deletes_only_the_service_window() -> None:
    client = _Recorder()
    purge_cmcd_projections(client, service_id="svc", since=NOW - timedelta(days=30))
    ((sql, params),) = client.calls
    assert sql.startswith("ALTER TABLE `cmcd_projection_facts` DELETE WHERE service_id={service_id:String}")
    assert "event_timestamp >= {since:DateTime64(3)}" in sql and "mutations_sync=2" in sql
    assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


@pytest.mark.parametrize(("domain", "table"), [("rum_vitals", "rum_vitals_facts"), ("rum_errors", "rum_error_facts")])
def test_purge_rum_facts_deletes_only_the_service_window_of_one_rum_table(domain: str, table: str) -> None:
    client = _Recorder()
    purge_rum_facts(client, domain=domain, service_id="svc", since=NOW - timedelta(days=30))
    ((sql, params),) = client.calls
    assert sql.startswith(f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}}")
    assert "event_timestamp >= {since:DateTime64(3)}" in sql and "mutations_sync=2" in sql
    assert params == {"service_id": "svc", "since": NOW - timedelta(days=30)}


def test_purge_rum_facts_rejects_other_domains() -> None:
    with pytest.raises(ValueError, match="rum_vitals or rum_errors"):
        purge_rum_facts(object(), domain="request", service_id="svc", since=NOW)  # type: ignore[arg-type]


def test_rum_fact_rebuild_replays_the_rum_domain_over_the_requested_window() -> None:
    rec = _manifest("rum-1", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},))
    warm = _manifest(
        "rum-2", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    _, archive, publication, _ = _pub_env(rec, warm)
    seen: list[str] = []

    class _TrackingCatalog:
        def manifests_covering(self, service_id, domain, start, end):
            seen.append(domain)
            return tuple(m for m in (rec[0], warm[0]) if m.coverage_end > start and m.coverage_start < end)

    report = rebuild_rum_facts_from_fos(
        _TrackingCatalog(),
        archive,
        publication,
        service_id="svc",
        domain="rum_vitals",
        now=NOW,
        window=timedelta(days=30),
    )
    assert set(seen) == {"rum_vitals"} and report.full_rebuild_complete and report.manifests_replayed == 2
    with pytest.raises(ValueError, match="rum_vitals or rum_errors"):
        rebuild_rum_facts_from_fos(
            _TrackingCatalog(), archive, publication, service_id="svc", domain="request", now=NOW
        )
    assert rebuild_rum_facts_from_fos(
        _TrackingCatalog(),
        archive,
        publication,
        service_id="svc",
        domain="rum_errors",
        now=NOW,
        window=timedelta(hours=6),
    ).full_rebuild_complete


def test_performance_projection_rebuild_backfills_asn_rows_but_never_republishes_url_rows() -> None:
    events = tuple({**event, "asn": 7922, "url": "/slow"} for event in _performance_events())
    rec = _manifest("asn-perf", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), events)
    catalog, archive, publication, client = _pub_env(rec)
    report = rebuild_performance_projections_from_fos(
        catalog, archive, publication, service_id="svc", now=NOW, rebuild_id="r1"
    )
    assert report.full_rebuild_complete
    assert client.insert_calls == [
        "rebuild-performance:r1:asn-perf:summary",
        "rebuild-performance:r1:asn-perf:scatter",
        "rebuild-performance:r1:asn-perf:asn",
    ]


def _make_archive_manifest(mid: str, domain: str, key: str, rows: tuple[dict, ...]):
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", domain, key, "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        mid,
        source,
        ArchiveArtifact(
            f"s3://archive/artifacts/{mid}.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            len(rows),
            _event_digest(rows),
            f"{domain}.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(manifest, artifact_bytes)
    return manifest, archive


def test_replay_rebuilds_visibility_from_archive_only() -> None:
    rows = ({"event_id": "event-1", "status": 200}, {"event_id": "event-2", "status": 500})
    manifest, archive = _make_archive_manifest("manifest-1", "request", "raw/request/a.gz", rows)
    result = replay_manifest(manifest, archive, ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient()))
    assert result.rows_read == 2 and result.publication.rows_visible == 2


def test_replay_with_a_custom_batch_id_reinserts_after_a_prior_replay_is_marked_visible() -> None:
    manifest, archive = _make_archive_manifest(
        "manifest-1", "request", "raw/request/a.gz", ({"event_id": "event-1", "status": 200},)
    )
    inserted: list[int] = []

    class CountingClient:
        def insert(self, batch):
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    publication = ClickHousePublication(InMemoryBatchManifestStore(), CountingClient())
    assert not replay_manifest(manifest, archive, publication).publication.duplicate and inserted == [1]
    assert replay_manifest(manifest, archive, publication).publication.duplicate and inserted == [1]
    assert not replay_manifest(
        manifest, archive, publication, batch_id=f"rebuild:xyz:{manifest.manifest_id}"
    ).publication.duplicate
    assert inserted == [1, 1]


def test_replay_manifest_filters_mixed_rum_rows_by_serving_domain() -> None:
    vitals_only = ({"event_id": "vital-1", "metric_name": "LCP", "metric_value": 1200.0, "error_message": ""},)
    manifest, archive = _make_archive_manifest("manifest-rum-err-empty", "rum_errors", "raw/rum/a.gz", vitals_only)
    inserted: list[int] = []

    class StrictRumErrorClient:
        def insert(self, batch):
            if any(not row.get("error_message") for row in batch.rows):
                raise ValueError("rum_errors batch contained non-error row")
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    result = replay_manifest(
        manifest, archive, ClickHousePublication(InMemoryBatchManifestStore(), StrictRumErrorClient())
    )
    assert result.rows_read == 0 and result.publication.rows_visible == 0 and inserted == []


def _rum_repair_archive(tmp_path, *, faro_body=None):
    b1 = faro_body or {"measurements": [{"type": "web-vitals", "values": {"lcp": 12, "cls": 0.1}}]}
    source_payload = (
        f"\n{json.dumps({'timestamp': '2026-09-14T11:00:00Z', 'rum_body': json.dumps(b1)})}\n"
        f"{json.dumps({'timestamp': '2026-09-14T11:00:01Z', 'rum_body': json.dumps({'exceptions': [{'value': 'boom'}]})})}"
    ).encode()
    source = ArchiveSourceObject(
        "svc", "rum_errors", "raw/rum/repair.gz", hashlib.sha256(source_payload).hexdigest(), len(source_payload), "v1"
    )
    decoded = decode_source_object(source, source_payload, transform_version="normalize.v1")
    rows = [dict(event, _record_kind="event") for event in decoded.events]
    manifest = write_batch_archive_checkpoint(
        tmp_path,
        service_id="svc",
        domain="rum_errors",
        source_batches=[(source, rows)],
        archive_epoch=9,
        schema_version="rum.v1",
        transform_version="normalize.v1",
        coverage_start=NOW,
        coverage_end=NOW,
    )
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(manifest, Path(manifest.artifact.uri.removeprefix("file://")).read_bytes())
    return manifest, archive


def test_scoped_rum_repair_uses_actual_standard_faro_event_projections(tmp_path) -> None:
    body = {
        "measurements": [{"type": "web-vitals", "values": {"lcp": 12, "cls": 0.1, "delta": 9}}],
        "events": [
            {"name": "click"},
            {"name": "faro.performance.navigation", "attributes": {"domContentLoaded": 40, "loadEvent": 50}},
        ],
    }
    manifest, archive = _rum_repair_archive(tmp_path, faro_body=body)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    vitals = [row for batch in plan.batches if batch.domain == "rum_vitals" for row in batch.rows]
    assert [(r["metric_name"], r["metric_value"]) for r in vitals] == [
        (m["metric_name"], m["metric_value"]) for m in extract_metrics_from_faro_payload(body, {})
    ]
    assert plan.candidate_counts == {"rum_vitals": 5, "rum_errors": 1}
    assert next(r for r in vitals if r["metric_name"] == "event_click")["metric_value"] == 1.0
    assert all(r["metric_name"] not in {"exception", "delta"} for r in vitals)


def test_scoped_rum_repair_checks_other_batches_and_is_idempotent(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    empty = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    assert empty.candidate_counts == {"rum_vitals": 2, "rum_errors": 1}
    assert all(batch.generation == "2" for batch in empty.batches)
    assert {row["line_ordinal"] for batch in empty.batches for row in batch.rows} == {1, 2}

    persisted = [
        dict(row, repair_visible=True, batch_id="00000000-0000-0000-0000-000000000099") for row in empty.expected_facts
    ]
    errors_only = [row for row in persisted if "error_message" in row]
    plan = plan_rum_recovery(
        manifest, archive, owner_epoch=2, read_facts=lambda d, _: errors_only if d == "rum_errors" else (), now=NOW
    )
    assert plan.missing_counts == {"rum_vitals": 2, "rum_errors": 0}
    assert plan.exact_identity_counts == {"rum_vitals": 0, "rum_errors": 1} and len(plan.batches) == 1
    assert plan.plan_digest == empty.plan_digest and plan.batches[0].batch_id == empty.batches[0].batch_id
    complete = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    assert complete.batches == () and complete.missing_counts == {"rum_vitals": 0, "rum_errors": 0}

    for index, fact in enumerate(persisted, start=1):
        fact.update(event_id=f"00000000-0000-0000-0000-{index:012x}", transform_version="older-transform")
    equivalent = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    assert equivalent.batches == () and equivalent.equivalent_identity_counts == {"rum_vitals": 2, "rum_errors": 1}


@pytest.mark.parametrize("visible,copies", [(False, 1), (True, 2)])
def test_scoped_rum_repair_refuses_hidden_or_duplicate_existing_identity(tmp_path, visible, copies) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    fact = dict(plan.expected_facts[0], repair_visible=visible)
    with pytest.raises(ValueError, match="ambiguous or unpublished"):
        plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: [fact] * copies, now=NOW)


def test_scoped_rum_repair_refuses_unretained_event_times(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    with pytest.raises(ValueError, match="outside retained"):
        plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW + timedelta(days=31))


@pytest.mark.parametrize("domain", ["rum_vitals", "rum_errors"])
def test_scoped_rum_content_normalizes_float64_json_integer_representation(domain) -> None:
    expected = {"event_timestamp": NOW, "ttfb": 25.0, "metric_value": 12.0}
    physical = {"event_timestamp": NOW, "ttfb": 25, "metric_value": 12}
    assert _content(expected, domain) == _content(physical, domain)
    assert _content(expected, domain) != _content(dict(physical, ttfb=26), domain)


def test_scoped_rum_apply_requires_reviewed_digest_and_verifies_retry(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    persisted: list[dict] = []
    make_plan = partial(plan_rum_recovery, manifest, archive, owner_epoch=2, read_facts=lambda *_: persisted, now=NOW)
    initial = make_plan()
    publication = MagicMock()
    assert execute_rum_recovery(make_plan, publication, apply=False)["applied"] is False
    publication.publish.assert_not_called()
    with pytest.raises(ValueError, match="reviewed plan"):
        execute_rum_recovery(make_plan, publication, apply=True, approve_digest="wrong")
    publication.publish.assert_not_called()

    def publish(batch):
        ids = {_event_uuid(row["event_id"], field="event_id") for row in batch.rows}
        persisted.extend(dict(row, repair_visible=True) for row in initial.expected_facts if row["event_id"] in ids)

    publication.publish.side_effect = publish
    result = execute_rum_recovery(make_plan, publication, apply=True, approve_digest=initial.plan_digest)
    assert result["remaining_counts"] == {"rum_vitals": 0, "rum_errors": 0} and publication.publish.call_count == 2
    execute_rum_recovery(make_plan, publication, apply=True, approve_digest=initial.plan_digest)
    assert publication.publish.call_count == 2


def test_scoped_rum_cli_defaults_to_dryrun_and_redacts_dependency_errors(monkeypatch, capsys) -> None:
    from scripts import recover_high_scale_rum as cli

    run = MagicMock(return_value={"applied": False, "candidate_counts": {"rum_vitals": 2, "rum_errors": 1}})
    monkeypatch.setattr(cli, "recover", run)
    assert cli.main(["--manifest-prefix", "a" * 8]) == 0
    run.assert_called_once_with("a" * 8, apply=False, approve_digest=None)
    assert json.loads(capsys.readouterr().out)["applied"] is False
    run.side_effect = ValueError("private dependency response must not escape")
    assert cli.main(["--manifest-prefix", "a" * 8]) == 1
    assert json.loads(capsys.readouterr().out) == {"status": "blocked", "error_type": "ValueError"}


def test_scoped_rum_cli_uses_canonical_source_owner_epoch_column(tmp_path, monkeypatch) -> None:
    from scripts import recover_high_scale_rum as cli

    manifest, archive = _rum_repair_archive(tmp_path)
    connection = MagicMock()
    connection.__enter__.return_value = connection

    def execute(query, params=None):
        cursor = MagicMock()
        if "FROM high_scale_archive_manifests" in query:
            cursor.fetchall.return_value = [{}]
        elif "FROM high_scale_ownership" in query:
            cursor.fetchone.return_value = {"owner_epoch": 2, "current_owner": "high_scale"}
        elif "FROM high_scale_source_objects" in query:
            assert "archive_owner_epoch" not in query and "archive_manifest_id,owner_epoch" in query
            cursor.fetchall.return_value = [
                {
                    "object_key": s.key,
                    "status": "source_deleted",
                    "archive_manifest_id": manifest.manifest_id,
                    "owner_epoch": 2,
                }
                for s in manifest.sources
            ]
        return cursor

    connection.execute.side_effect = execute
    monkeypatch.setenv("METADATA_DSN", "postgresql://unused")
    monkeypatch.setattr(psycopg, "connect", MagicMock(return_value=connection))
    monkeypatch.setattr(
        postgres_control,
        "_archive_from_row",
        lambda _: SimpleNamespace(manifest=manifest, state="source_deleted", owner_epoch=2),
    )
    monkeypatch.setattr(postgres_control, "PostgresControlPlane", MagicMock())
    monkeypatch.setattr(registry_bootstrap, "_default_archive_factory", lambda _: archive)
    monkeypatch.setattr(
        config, "load_clickhouse_config", lambda: make_dataclass("RecoveryTestSettings", [("query_timeout_s", int)])(5)
    )
    client = MagicMock()
    initial_plan = rum_recovery.plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    error_fact = next(fact for fact in initial_plan.expected_facts if fact["_repair_domain"] == "rum_errors")

    def read_clickhouse(query, params=None, *, settings=None):
        if "high_scale_batch_publications" in query:
            assert "batch_id={batch_id:String}" in query and "batch_id:UUID" not in query
            return [{"publication_state": "visible", "expected_rows": 1, "visible_rows": 1}]
        return [dict(error_fact)] if "rum_error_facts" in query else []

    client.execute.side_effect = read_clickhouse
    monkeypatch.setattr(clickhouse_client, "ClickHouseClient", MagicMock(return_value=client))
    monkeypatch.setattr(pub_mod, "PostgresBatchManifestStore", MagicMock())
    serving = MagicMock()
    monkeypatch.setattr(pub_mod, "ClickHousePublication", MagicMock(return_value=serving))
    original_plan = rum_recovery.plan_rum_recovery
    monkeypatch.setattr(rum_recovery, "plan_rum_recovery", lambda *a, **kw: original_plan(*a, **kw, now=NOW))

    report = cli.recover("a" * 8, apply=False, approve_digest=None)
    assert report["read_only"] is True
    assert report["candidate_counts"] == {"rum_vitals": 2, "rum_errors": 1}
    assert report["missing_counts"] == {"rum_vitals": 2, "rum_errors": 0}
    serving.publish.assert_not_called()


def test_scoped_rum_apply_cannot_report_success_on_missing_insert(tmp_path) -> None:
    manifest, archive = _rum_repair_archive(tmp_path)
    make_plan = partial(plan_rum_recovery, manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    with pytest.raises(ValueError, match="post-apply identity verification"):
        execute_rum_recovery(make_plan, MagicMock(), apply=True, approve_digest=make_plan().plan_digest)


class _PartitionedRumClient:
    def __init__(self, rows=()):
        self.rows = [dict(row) for row in rows]
        self.partitions = {"20260901": 71_167, "20260905": 60_000, "20260914": 70_000}
        self.queries, self.inserts = [], []
        self.receipt = None
        self.truncate_insert = self.change_inventory = False
        self.inventory_reads = 0

    def execute(self, query, params=None, *, settings=None):
        self.queries.append((query, params, settings))
        if "system.parts" in query:
            self.inventory_reads += 1
            rows = [{"partition_id": k, "partition_rows": c} for k, c in self.partitions.items()]
            if self.change_inventory and self.inventory_reads % 2 == 0:
                rows.append({"partition_id": "20260930", "partition_rows": 1})
            return rows
        if "high_scale_batch_publications" in query:
            return [] if self.receipt is None else [dict(self.receipt)]
        assert "_partition_id IN" in query and "event_timestamp >" not in query and "source_object_key IN" not in query
        partitions = {v for k, v in params.items() if k.startswith("part")}
        return [
            dict(r)
            for r in self.rows
            if r["event_timestamp"].strftime("%Y%m%d") in partitions
            and str(r["batch_id"]) == params["batch_id"]
            and r["service_id"] == params["service_id"]
        ]

    def insert_rows(self, table, columns, rows):
        self.inserts.append((table, len(rows)))
        if table == "high_scale_batch_publications":
            self.receipt = dict(zip(columns, rows[0], strict=True))
        else:
            self.rows.extend(dict(zip(columns, r, strict=True)) for r in (rows[:1] if self.truncate_insert else rows))

    def delete_batch_rows(self, *_):
        raise AssertionError("repair must never silently delete a partial/conflicting batch")


def _partitioned_rum_adapter(tmp_path, *, physical=()):
    manifest, archive = _rum_repair_archive(tmp_path)
    plan = plan_rum_recovery(manifest, archive, owner_epoch=2, read_facts=lambda *_: (), now=NOW)
    batch = next(b for b in plan.batches if b.domain == "rum_vitals")
    rows = [
        dict(zip(_DOMAIN_COLUMNS[batch.domain], r, strict=True)) for r in _rows_for_domain(batch, _batch_uuid(batch))
    ]
    client = _PartitionedRumClient(physical)
    return RumRecoveryBatchAdapter(client), client, batch, rows


def test_partitioned_rum_count_preserves_global_empty_insert_and_retry(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    assert adapter.inspect_batch(batch) == (0, None)
    assert adapter.insert(batch).rows_inserted == 2 and adapter.insert(batch).rows_inserted == 2
    assert len(client.rows) == 2 and client.inserts.count(("rum_vitals_facts", 2)) == 1
    selected_partitions = set()
    for query, params, settings in client.queries:
        if "system.parts" not in query and "high_scale_batch_publications" not in query:
            selected_partitions.update(v for k, v in params.items() if k.startswith("part"))
        if settings is not None:
            assert settings["max_rows_to_read"] == "100000" and settings["read_overflow_mode"] == "throw"
    assert selected_partitions == set(client.partitions)


@pytest.mark.parametrize("kind", "partial duplicate conflict outside_time outside_source unknown_id".split())
def test_partitioned_rum_count_refuses_bad_existing_rows_without_writes(tmp_path, kind) -> None:
    adapter, client, batch, rows = _partitioned_rum_adapter(tmp_path)
    if kind == "partial":
        rows = rows[:1]
    elif kind == "duplicate":
        rows = [rows[0], rows[0]]
    elif kind == "conflict":
        rows[0]["metric_value"] = 999
    elif kind == "outside_time":
        rows[0]["event_timestamp"] = NOW - timedelta(days=13)
    elif kind == "outside_source":
        rows[0]["source_object_key"] = "raw/rum/unexpected.gz"
    else:
        rows[0].update(event_id="00000000-0000-0000-0000-000000000099", event_timestamp=NOW - timedelta(days=13))
    client.rows = rows
    with pytest.raises((ValueError, PartialInsert)):
        adapter.insert(batch)
    assert client.inserts == []


def test_partitioned_rum_count_does_not_delete_or_publish_partial_insert(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    client.truncate_insert = True
    with pytest.raises(PartialInsert):
        adapter.insert(batch)
    assert client.receipt["publication_state"] == "pending" and len(client.rows) == 1
    before = list(client.inserts)
    with pytest.raises(PartialInsert):
        adapter.insert(batch)
    assert client.inserts == before


def test_partitioned_rum_count_checks_physical_rows_even_with_visible_receipt(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    adapter.insert(batch)
    client.rows.append(dict(client.rows[0]))
    before = list(client.inserts)
    with pytest.raises(ValueError, match="duplicate"):
        adapter.insert(batch)
    assert client.inserts == before


def test_partitioned_rum_count_recovers_full_valid_lost_ack_without_reinserting(tmp_path) -> None:
    adapter, client, batch, rows = _partitioned_rum_adapter(tmp_path)
    client.rows = rows
    assert adapter.inspect_batch(batch) == (2, None) and adapter.insert(batch).rows_inserted == 2
    assert not any(table == "rum_vitals_facts" for table, _ in client.inserts)
    assert client.receipt["publication_state"] == "visible"


def test_partitioned_rum_count_never_swallows_failed_partition_read(tmp_path) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    original = client.execute
    client.execute = lambda q, *a, **kw: (
        (_ for _ in ()).throw(RuntimeError("bounded scan failed")) if "_partition_id IN" in q else original(q, *a, **kw)
    )
    with pytest.raises(RuntimeError, match="bounded scan failed"):
        adapter.inspect_batch(batch)
    assert client.inserts == []


@pytest.mark.parametrize("unsafe", "changing_inventory oversized_partition too_many_groups too_many_partitions".split())
def test_partitioned_rum_count_refuses_unprovable_inventory(tmp_path, unsafe) -> None:
    adapter, client, batch, _ = _partitioned_rum_adapter(tmp_path)
    if unsafe == "changing_inventory":
        client.change_inventory = True
    elif unsafe == "oversized_partition":
        client.partitions = {"20260914": 100_001}
    elif unsafe == "too_many_groups":
        client.partitions = {f"partition-{i}": 90_000 for i in range(7)}
    else:
        client.partitions = {f"partition-{i}": 1 for i in range(33)}
    with pytest.raises(ValueError):
        adapter.inspect_batch(batch)
    assert client.inserts == []


_GOOD = dict(
    events_per_second=2_000_000,
    recovery_window_seconds=900,
    archive_read_events_per_second=300_000_000,
    decode_events_per_second=300_000_000,
    aggregate_events_per_second=300_000_000,
    clickhouse_insert_events_per_second=750_000_000,
    replication_factor=3,
    live_ingest_reservation=0.2,
)


def _measurement(**overrides) -> RecoveryCapacityMeasurement:
    return RecoveryCapacityMeasurement(**{**_GOOD, **overrides})


def _recovery_env(*, fail_on: set[str] | None = None):
    return _pub_env(*_recent_and_warm(), fail_on=fail_on)


def _rebuild(env, **kwargs):
    catalog, archive, publication, _ = env
    return rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, **kwargs
    )


def test_feasible_measurement_with_a_complete_rebuild_makes_the_claim_valid() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env(), capacity=_measurement())
    assert report.full_rebuild_complete and report.capacity is not None and report.capacity.feasible
    assert report.recovery_claim_valid is True
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["feasible"] is True and gate["log_level"] == "info"


def test_infeasible_measurement_flags_the_claim_but_never_blocks_recovery() -> None:
    env = _recovery_env()
    with capture_logs() as logs:
        report = _rebuild(env, capacity=_measurement(clickhouse_insert_events_per_second=400_000_000))
    assert report.full_rebuild_complete and report.rows_replayed == 2 and len(env[3].insert_calls) == 2
    assert report.capacity is not None and report.capacity.limiting_stage == "clickhouse_insert"
    assert report.recovery_claim_valid is False
    gate = next(e for e in logs if e["event"] == "high_scale.recovery_capacity_gate")
    assert gate["log_level"] == "warning" and gate["limiting_stage"] == "clickhouse_insert"


def test_feasible_capacity_does_not_validate_an_incomplete_rebuild() -> None:
    report = _rebuild(_recovery_env(fail_on={"warm"}), capacity=_measurement())
    assert report.capacity is not None and report.capacity.feasible and not report.full_rebuild_complete
    assert report.recovery_claim_valid is False


def test_without_a_measurement_the_report_carries_no_claim() -> None:
    with capture_logs() as logs:
        report = _rebuild(_recovery_env())
    assert report.capacity is None and report.recovery_claim_valid is None
    assert not [e for e in logs if e["event"] == "high_scale.recovery_capacity_gate"]


def test_invalid_measurement_fails_before_any_replay_work() -> None:
    env = _recovery_env()
    with pytest.raises(ValueError, match="event rate and recovery window must be positive"):
        _rebuild(env, capacity=_measurement(events_per_second=0))
    assert env[3].insert_calls == []


def test_origin_projection_rebuild_evaluates_the_same_gate() -> None:
    catalog, archive, publication, _ = _recovery_env()
    report = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
        window=timedelta(hours=2),
        capacity=_measurement(decode_events_per_second=1_000),
    )
    assert (
        report.capacity is not None and report.capacity.limiting_stage == "decode" and not report.recovery_claim_valid
    )


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale as script

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    registry = MagicMock()
    registry.resolve.return_value = service
    monkeypatch.setattr(script.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(script, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(script, "get_high_scale_service_registry", lambda: registry)
    monkeypatch.setattr(script, "register_high_scale_services_from_environment", lambda *a, **k: None)
    monkeypatch.setattr(script, "PostgresControlPlane", MagicMock)
    monkeypatch.setattr(script, "PostgresBatchManifestStore", lambda control: MagicMock())
    monkeypatch.setattr(script, "AggregateBatchAdapter", lambda client: MagicMock())
    monkeypatch.setattr(script, "ClickHousePublication", lambda manifests, adapter: MagicMock())
    return script


def _run_cli(cli, monkeypatch, tmp_path, measurement: dict | None, *, complete: bool = True):
    seen: dict = {}

    def fake_rebuild(*args, **kwargs):
        seen.update(kwargs)
        cap = kwargs.get("capacity")
        rep = evaluate_recovery_capacity(cap) if cap else None
        return RecoveryReport(
            True,
            complete,
            True,
            complete,
            complete,
            1,
            1,
            (),
            "rb-1",
            capacity=rep,
            recovery_claim_valid=None if rep is None else rep.feasible and complete,
        )

    monkeypatch.setattr(cli, "rebuild_origin_projections_from_fos", fake_rebuild)
    argv = ["backfill", "--service-id", "svc"]
    if measurement is not None:
        path = tmp_path / "capacity.json"
        path.write_text(json.dumps(measurement))
        argv += ["--capacity-json", str(path)]
    monkeypatch.setattr("sys.argv", argv)
    return cli.main(), seen


def test_cli_capacity_and_exit_codes(cli, monkeypatch, tmp_path) -> None:
    code, seen = _run_cli(cli, monkeypatch, tmp_path, _GOOD)
    assert code == 0 and seen["capacity"] == RecoveryCapacityMeasurement(**_GOOD)
    assert _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "decode_events_per_second": 1_000})[0] == 3
    assert _run_cli(cli, monkeypatch, tmp_path, None)[0] == 0
    assert _run_cli(cli, monkeypatch, tmp_path, None, complete=False)[0] == 1

    with pytest.raises(SystemExit) as excinfo:
        _run_cli(cli, monkeypatch, tmp_path, {**_GOOD, "events_per_second": 0})
    assert excinfo.value.code == 2


def test_cli_replace_existing_and_projection_routing(cli, monkeypatch, tmp_path) -> None:
    order: list[str] = []
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kw: order.append(f"purge:{kw['service_id']}"))
    monkeypatch.setattr(
        cli,
        "rebuild_origin_projections_from_fos",
        lambda *a, **kw: order.append("rebuild") or RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1"),
    )
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing"])
    assert cli.main() == 0 and order == ["purge:svc", "rebuild"]

    purged: list[str] = []
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kw: purged.append("purge"))
    _run_cli(cli, monkeypatch, tmp_path, None)
    assert purged == []

    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--replace-existing", "--rebuild-id", "rb-1"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 2

    calls, adapters = [], []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "AggregateBatchAdapter", lambda client: adapters.append("aggregate") or MagicMock())
    monkeypatch.setattr(cli, "purge_cmcd_projections", lambda client, **kw: calls.append("purge-cmcd"))
    monkeypatch.setattr(cli, "purge_origin_projections", lambda client, **kw: calls.append("purge-origin"))
    monkeypatch.setattr(
        cli,
        "rebuild_cmcd_projections_from_fos",
        lambda *a, **kw: calls.append("rebuild-cmcd") or RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1"),
    )
    monkeypatch.setattr(
        cli,
        "rebuild_origin_projections_from_fos",
        lambda *a, **k: pytest.fail("origin rebuild must not run for --projection cmcd"),
    )
    monkeypatch.setattr("sys.argv", ["backfill", "--service-id", "svc", "--projection", "cmcd", "--replace-existing"])
    assert cli.main() == 0 and calls == ["purge-cmcd", "rebuild-cmcd"] and adapters == ["facts"]

    seen, rum_adapters = {}, []
    monkeypatch.setattr(cli, "ClickHouseBatchAdapter", lambda client: rum_adapters.append("facts") or MagicMock())
    monkeypatch.setattr(cli, "purge_rum_facts", lambda client, **kw: seen.setdefault("purge", kw))
    monkeypatch.setattr(
        cli,
        "rebuild_rum_facts_from_fos",
        lambda *a, **kw: (
            seen.setdefault("rebuild", kw) and RecoveryReport(True, True, True, True, True, 1, 1, (), "rb-1")
        ),
    )
    monkeypatch.setattr(
        "sys.argv", ["backfill", "--service-id", "svc", "--projection", "rum_errors", "--replace-existing"]
    )
    assert cli.main() == 0 and rum_adapters == ["facts"]
    assert seen["purge"]["domain"] == "rum_errors" and seen["purge"]["service_id"] == "svc"
    assert seen["rebuild"]["domain"] == "rum_errors" and seen["rebuild"]["service_id"] == "svc"
