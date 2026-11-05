"""Tests for ``backend.routers.dashboard``, ``query``, and ``rum``."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import time as _time
import typing
from datetime import UTC
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

import backend.high_scale.dashboard as hsd
import backend.high_scale.ingest_controller as hsreg
import backend.routers.dashboard as dash
from backend.core import request_telemetry as web_vitals_store
from backend.core.request_context import RequestContext, build_request_context
from backend.main import app
from backend.models.analytics import AggregatesRequest, QueryRequest
from backend.provision.rum_orchestrator import upgrade_faro_version
from backend.routers import rum as rum_router
from backend.routers.query import _redact_paths
from backend.routers.rum import router as _rum_api_router
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import TimeBounds, _is_blocked_path, get_analyst_time_bounds
from backend.utils.telemetry import get_queries
from backend.utils.time_window import resolve_window
from tests._helpers.config_fakes import mutate_via_save
from tests.conftest import MOCK_SERVICE_ID, make_test_request_context, override_request_context

_BUNDLE_BODY: dict[str, Any] = {
    "start_time": "2026-06-12T00:00:00Z",
    "end_time": "2026-06-12T01:00:00Z",
    "filters": {},
    "chart_metric": "requests",
    "chart_interval": "minute",
}
_CSV_BODY: dict[str, Any] = {
    "start_time": "2026-06-12T00:00:00Z",
    "end_time": "2026-06-12T01:00:00Z",
    "filters": {},
}
_Q_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}


class _AttrDict(dict):
    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value


@pytest.fixture(autouse=True)
def _default_high_scale_service(monkeypatch):
    def _resolve(sid: str):
        client = SimpleNamespace(
            responses={},
            execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in str(sql) else [],
        )
        return SimpleNamespace(service_id=sid, client=client, watermark=lambda: None)

    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=_resolve),
    )


@pytest.mark.parametrize("endpoint", ["aggregates", "bundle"])
def test_high_scale_receives_analyst_clamp(client, endpoint, stub_aggregates, test_service_source):
    start = datetime.datetime(2026, 9, 1, 6, tzinfo=UTC)
    end = datetime.datetime(2026, 9, 1, 8, tzinfo=UTC)
    ctx = make_test_request_context(
        service_id=test_service_source["service_id"],
        source=test_service_source,
        session=SimpleNamespace(service_ids=[test_service_source["service_id"]]),
        path=f"/api/dashboard/{endpoint}",
        time_bounds=TimeBounds(start=start, end=end),
    )
    app.dependency_overrides[build_request_context] = lambda: ctx
    resp = client.post(
        f"/api/dashboard/{endpoint}",
        json={
            "fields": ["country"],
            "sections": ["core", "topten"],
            "filters": {},
            "chart_interval": "1 hour",
            "chart_metric": "requests",
            "start_time": "2026-08-01T00:00:00Z",
            "end_time": "2026-09-02T00:00:00Z",
        },
    )
    assert resp.status_code == 200, resp.text
    stub_aggregates.assert_called_once()
    _, _, called_start, called_end = stub_aggregates.call_args.args
    assert datetime.datetime.fromisoformat(called_start.replace("Z", "+00:00")) == start
    assert datetime.datetime.fromisoformat(called_end.replace("Z", "+00:00")) == end


def test_invalid_filter_rejected_before_dispatch(client, monkeypatch):
    dispatch = MagicMock(side_effect=AssertionError("invalid request dispatched"))
    monkeypatch.setattr("backend.high_scale.dashboard.aggregates", dispatch)
    resp = client.post(
        "/api/dashboard/aggregates", json={"filters": {"country": {"mode": "invalid", "values": ["US"]}}}
    )
    assert resp.status_code == 422
    dispatch.assert_not_called()


@pytest.fixture
def stub_aggregates(monkeypatch):
    ret = _AttrDict(
        {
            "data": {},
            "time_series": [],
            "map_data": [],
            "where_clause": "1=1",
            "interval": "minute",
            "metric": "requests",
            "total_rows": 100,
            "total_rows_total": 200,
            "section_timings": [{"section": "agg:query", "time_ms": 5.0}],
            "debug_queries": [{"sql": "SELECT 1", "time_ms": 1.2, "rows": 1, "caller": "agg_caller"}],
            "debug_calls": [
                {"service": "fastly", "method": "GET", "path": "/stats", "time_ms": 0.5, "caller": "agg_caller"}
            ],
        }
    )

    def _side_effect(*_a, **_kw):
        get_queries().append({"sql": "SELECT 1", "time_ms": 1.2, "rows": 1, "caller": "agg_caller"})
        return _AttrDict(stub.return_value)

    stub = MagicMock(side_effect=_side_effect, return_value=ret)
    monkeypatch.setattr("backend.high_scale.dashboard.aggregates", stub)
    return stub


@pytest.fixture
def stub_top_bots(monkeypatch):
    ret = _AttrDict(
        {
            "bots": [{"name": "Googlebot", "request_count": 42}],
            "ngwaf_bots": [],
            "section_timings": [{"section": "bots:query", "time_ms": 3.0}],
            "debug_queries": [{"sql": "SELECT bot", "time_ms": 0.5, "rows": 1, "caller": "bots_caller"}],
            "debug_calls": [],
        }
    )

    def _side_effect(*_a, **_kw):
        get_queries().append({"sql": "SELECT bot", "time_ms": 0.5, "rows": 1, "caller": "bots_caller"})
        return _AttrDict(stub.return_value)

    stub = MagicMock(side_effect=_side_effect, return_value=ret)
    monkeypatch.setattr("backend.high_scale.security.top_bots", stub)
    return stub


def test_bundle_preserves_and_omits_component_owned_reader_coverage(
    client, stub_aggregates, stub_top_bots, monkeypatch
):
    body_unobs = client.post("/api/dashboard/bundle", json={"filters": {}}).json()
    assert "_reader_coverage" not in body_unobs["aggregates"] and "_reader_coverage" not in body_unobs["top_bots"]

    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    stub_aggregates.return_value["reader_coverage"] = [
        {
            "reader": "dashboard.conn_requests_histogram",
            "minimum_window_hours": 2,
            "eligible": False,
            "eligibility_reason": "window_too_short",
            "outcome": "ineligible",
        }
    ]
    stub_top_bots.return_value["reader_coverage"] = [
        {
            "reader": "dashboard.ngwaf_bots",
            "minimum_window_hours": 2,
            "eligible": True,
            "eligibility_reason": "eligible",
            "outcome": "served",
            "attempts": [{"bundle": "ngwaf_bots", "reason": "planned", "closed_hours": 4, "covered_hours": 2}],
        }
    ]
    response = client.post("/api/dashboard/bundle", json={"filters": {}})
    assert response.status_code == 200
    body = response.json()
    assert body["aggregates"]["_reader_coverage"][0]["reader"] == "dashboard.conn_requests_histogram"
    assert body["top_bots"]["_reader_coverage"][0]["reader"] == "dashboard.ngwaf_bots"
    assert body["top_bots"]["_reader_coverage"][0]["attempts"][0]["covered_hours"] == 2
    assert "_reader_coverage" not in body
    assert "_debug_queries" not in body["aggregates"] and "_debug_queries" not in body["top_bots"]


# ── /api/dashboard/bundle ─────────────────────────────────────────────────────


@pytest.mark.parametrize("fields", [None, ["country", "url"], ["country", "_bot_name"]])
def test_bundle_returns_subresponses_and_always_fetches_top_bots(client, stub_aggregates, stub_top_bots, fields):
    payload = {**_BUNDLE_BODY, **({"fields": fields} if fields is not None else {})}
    resp = client.post("/api/dashboard/bundle", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    stub_aggregates.assert_called_once()
    stub_top_bots.assert_called_once()
    assert body["top_bots"]["bots"] == stub_top_bots.return_value["bots"]
    assert body["top_bots"]["ngwaf_bots"] == stub_top_bots.return_value["ngwaf_bots"]
    sections = [s["section"] for s in body["_section_timings"]]
    assert "agg:query" in sections and "bots:query" in sections
    assert "_section_timings" in body["aggregates"] and "section_timings" not in body["aggregates"]
    assert "_section_timings" in body["top_bots"]


@pytest.mark.parametrize("debug_on", [True, False])
def test_bundle_debug_keys_lifting_and_redaction(client, stub_aggregates, stub_top_bots, monkeypatch, debug_on):
    if debug_on:
        monkeypatch.setenv("DEBUG_RESPONSES", "1")
    else:
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
    body = client.post("/api/dashboard/bundle", json=_BUNDLE_BODY).json()
    if debug_on:
        sql_texts = [q["sql"] for q in body["_debug_queries"]]
        assert len(sql_texts) == 2 and "SELECT 1" in sql_texts and "SELECT bot" in sql_texts
    else:
        assert not body.get("_debug_queries") and not body.get("_debug_calls")
        for sub_name in ("aggregates", "top_bots"):
            sub = body[sub_name]
            assert not sub.get("_debug_queries") and not sub.get("_debug_calls")
            assert "debug_queries" not in sub and "debug_calls" not in sub


# ── /api/dashboard/raw/csv ───────────────────────────────────────────────────


@pytest.mark.parametrize("row_count", [0, 1, 2500])
def test_raw_csv_streaming_shapes(client, monkeypatch, row_count: int):
    if row_count == 0:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(
            {
                "timestamp": [f"2026-06-12T00:{i // 60:02d}:{i % 60:02d}Z" for i in range(row_count)],
                "status": [200] * row_count,
            }
        )
    monkeypatch.setattr("backend.high_scale.dashboard.raw_df", lambda *a, **kw: df)
    resp = client.post("/api/dashboard/raw/csv", json=_CSV_BODY)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    if row_count == 0:
        assert resp.text == ""
    else:
        assert "attachment" in resp.headers.get("content-disposition", "")
        assert resp.text.count("timestamp,status") == 1
        assert len([ln for ln in resp.text.split("\n") if ln]) == row_count + 1


def _inject_mask_ips_analyst(test_service_source):
    session = SimpleNamespace(service_ids=[test_service_source["service_id"]], pii_policy={"mask_ips": True})
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source, session=session
    )


@pytest.mark.security_regression
def test_raw_csv_masks_ip_and_redacts_cookie_session_for_masking_analyst(client, monkeypatch, test_service_source):
    df = pd.DataFrame(
        {"ip": ["203.0.113.45", "198.51.100.7"], "cookie_session": ["a1b2c3d4deadbeef", ""], "url": ["/a", "/b"]}
    )
    monkeypatch.setattr("backend.high_scale.dashboard.raw_df", lambda *a, **kw: df)
    _inject_mask_ips_analyst(test_service_source)

    resp = client.post("/api/dashboard/raw/csv", json=_CSV_BODY)
    assert resp.status_code == 200, resp.text
    text = resp.text
    assert "deadbeef" not in text and "203.0.113.45" not in text
    assert "[redacted]" in text and "203.0.113.xxx" in text
    assert "/a" in text and "/b" in text


@pytest.mark.security_regression
def test_raw_csv_keeps_raw_cookie_session_for_non_masking_caller(client, monkeypatch):
    df = pd.DataFrame({"cookie_session": ["a1b2c3d4deadbeef"], "url": ["/a"]})
    monkeypatch.setattr("backend.high_scale.dashboard.raw_df", lambda *a, **kw: df)

    resp = client.post("/api/dashboard/raw/csv", json=_CSV_BODY)
    assert resp.status_code == 200, resp.text
    assert "a1b2c3d4deadbeef" in resp.text


# ── /api/dashboard/bundle + /aggregates sections selector ─────────────────────


@pytest.mark.parametrize(("endpoint", "bad_section"), [("bundle", "not_a_section"), ("aggregates", "unknown_section")])
def test_dashboard_rejects_unknown_section(client, stub_aggregates, stub_top_bots, endpoint, bad_section):
    resp = client.post(f"/api/dashboard/{endpoint}", json={**_BUNDLE_BODY, "sections": [bad_section]})
    assert resp.status_code in (400, 422)


@pytest.mark.parametrize(
    ("endpoint", "sections", "exp_agg", "exp_bots", "exp_page_flags", "exp_top_n"),
    [
        ("bundle", ["core"], True, False, True, False),
        ("bundle", ["topten"], True, False, False, True),
        ("bundle", ["bots"], False, True, None, None),
        ("bundle", ["core", "bots"], True, True, True, False),
        ("aggregates", ["topten"], True, False, False, True),
    ],
)
def test_dashboard_sections_selector_routing(
    client, stub_aggregates, stub_top_bots, endpoint, sections, exp_agg, exp_bots, exp_page_flags, exp_top_n
):
    resp = client.post(f"/api/dashboard/{endpoint}", json={**_BUNDLE_BODY, "sections": sections})
    assert resp.status_code == 200
    body = resp.json()
    assert stub_aggregates.called is exp_agg
    assert stub_top_bots.called is exp_bots
    if exp_agg:
        called_req = stub_aggregates.call_args.args[1]
        assert dash._resolve_aggregate_flags(called_req, set(sections)) == (
            exp_page_flags,
            exp_page_flags,
            exp_page_flags,
            exp_top_n,
        )
    elif endpoint == "bundle":
        assert body.get("aggregates") is None
    if endpoint == "bundle":
        if exp_bots:
            assert body["top_bots"]["bots"] == [{"name": "Googlebot", "request_count": 42}]
        else:
            assert body.get("top_bots") is None


async def _measure_heartbeat_ticks(coro) -> tuple[Any, int]:
    ticks = 0

    async def _heartbeat():
        nonlocal ticks
        for _ in range(500):
            await asyncio.sleep(0.005)
            ticks += 1

    hb = asyncio.create_task(_heartbeat())
    try:
        res = await coro
    finally:
        hb.cancel()
    return res, ticks


async def test_high_scale_bundle_runs_off_event_loop(monkeypatch):
    sentinel_service = object()
    monkeypatch.setattr(
        hsreg, "get_high_scale_service_registry", lambda: SimpleNamespace(resolve=lambda _sid: sentinel_service)
    )

    def _blocking_bundle(service, req, start, end):
        assert service is sentinel_service
        _time.sleep(0.25)
        return {"aggregates": {}, "top_bots": {}}

    monkeypatch.setattr(hsd, "bundle", _blocking_bundle)
    ctx = RequestContext(
        service_id="svc-highscale",
        source={"name": "svc", "service_id": "svc-highscale"},
        telemetry=MagicMock(),
    )
    req = AggregatesRequest(**_BUNDLE_BODY, sections=["core"])
    result, ticks = await _measure_heartbeat_ticks(dash.dashboard_bundle(req, ctx))
    assert result["aggregates"] == {} and ticks >= 10


# ── Merged from tests/routers/test_query_router.py ──


def _stub_result() -> dict:
    return {
        "columns": [],
        "data": [],
        "row_count": 0,
        "total_rows": 0,
        "truncated": False,
        "elapsed_ms": 0,
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }


@pytest.mark.parametrize("sql", ["", "   \n\t"])
def test_query_empty_or_whitespace_sql_returns_400(client, sql: str):
    resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": sql})
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "empty_sql" and resp.json()["detail"]["message"] == "No SQL provided"


@pytest.mark.parametrize(
    ("side_effect", "sql", "exp_status", "exp_err", "exp_substr", "forbidden_substr"),
    [
        (
            PermissionError("not authorized for system catalog"),
            "SELECT 1",
            403,
            "sql_not_permitted",
            "not authorized",
            None,
        ),
        (
            RuntimeError("table 'nope' does not exist"),
            "SELECT * FROM nope",
            400,
            "query_failed",
            "does not exist",
            None,
        ),
        (
            RuntimeError("Cannot open file '/srv/data/buffers/x.parquet'"),
            "SELECT 1",
            400,
            "query_failed",
            "<path>",
            "/srv/",
        ),
    ],
)
def test_query_exception_mapping_and_path_redaction(
    client, side_effect, sql, exp_status, exp_err, exp_substr, forbidden_substr
):
    with patch("backend.high_scale.query.query_endpoint", side_effect=side_effect):
        resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": sql})
    assert resp.status_code == exp_status
    body = resp.json()["detail"]
    assert body["error"] == exp_err and exp_substr in body["message"]
    if forbidden_substr:
        assert forbidden_substr not in body["message"]


# ── _redact_paths: regex shape ──────────────────────────────────────────────


def test_redact_paths_rules():
    for prefix in ("/srv", "/var", "/tmp", "/data", "/home", "/opt", "/usr", "/mnt", "/app"):
        out = _redact_paths(f"Cannot open file '{prefix}/foo/bar.parquet'")
        assert prefix not in out and "<path>" in out

    out_prod = _redact_paths("IO Error: No files found that match '/app/data/services/abc123/raw/2026-06-17/x.parquet'")
    assert "/app/data" not in out_prod and "abc123" not in out_prod and "<path>" in out_prod

    for unredacted in (
        "lookup failed for https://example.com/var/foo on row 12",
        "column /api/foo missing",
        "ENOENT: /etc/hosts not found",
    ):
        assert _redact_paths(unredacted) == unredacted

    for msg, leaked in [
        ("Cannot open '/var/lib/clickhouse/foo.db'", "/var/lib/clickhouse/foo.db"),
        ('No such file: "/srv/cache/x.parquet"', "/srv/cache/x.parquet"),
        ("Path was /tmp/buffer.json on disk", "/tmp/buffer.json"),
        ("/data/services/svc.metadata.db corrupted", "/data/services/svc.metadata.db"),
    ]:
        assert leaked not in _redact_paths(msg)


def test_query_request_rejects_oversized_max_rows():
    for bad in (10_001, 0):
        with pytest.raises(ValidationError):
            QueryRequest(sql="SELECT 1", max_rows=bad)
    assert QueryRequest(sql="SELECT 1", max_rows=10_000).max_rows == 10_000


def test_admin_and_analyst_query_window_and_mask_forwarding(client, test_service_source):
    captured: dict = {}

    def _fake(service, req, start_time, end_time, mask_ips, **kw):
        captured.update(
            {
                "start_time": start_time,
                "end_time": end_time,
                "mask_ips": mask_ips,
                **kw,
            }
        )
        return _stub_result()

    with patch("backend.high_scale.query.query_endpoint", side_effect=_fake):
        resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT 1"})
    assert (
        resp.status_code == 200
        and captured["start_time"] is None
        and captured["end_time"] is None
        and captured["mask_ips"] is False
    )

    start = datetime.datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime.datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-1", pii_policy={"mask_ips": True}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )
    captured.clear()
    console_stub = SimpleNamespace(
        client_for=lambda sid, masked=True: SimpleNamespace(
            client=SimpleNamespace(), tables={"request_facts": "fla_cv_view_req"}
        )
    )
    with (
        patch("backend.high_scale.postgres_control.get_console_clients", return_value=console_stub),
        patch("backend.high_scale.query.query_endpoint", side_effect=_fake),
    ):
        resp2 = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT ip FROM logs"})
    assert resp2.status_code == 200, resp2.text
    assert (
        captured["start_time"] == start.isoformat()
        and captured["end_time"] == end.isoformat()
        and captured["mask_ips"] is True
    )


# ── /api/presets: source lookup + connection fallback ───────────────────────


def test_presets_source_resolution_and_no_connection_acquired(client):
    with patch("backend.core.request_context.svcconfig.get_active_service_id", return_value=None):
        resp_no_svc = client.get("/api/presets")
    assert resp_no_svc.status_code == 200 and resp_no_svc.json() == []

    with patch("backend.routers.query.get_source_for_service", return_value=None):
        resp_unknown = client.get("/api/presets", headers={"x-fastly-service-id": "ghost-service"})
    assert resp_unknown.status_code == 200 and resp_unknown.json() == []

    fake_presets = [{"name": "Top 5xx", "description": "Top 5xx URLs", "sql": "SELECT 1"}]
    with (
        patch("backend.routers.query.get_cached_presets", return_value=None),
        patch(
            "backend.routers.query.get_source_for_service",
            return_value={"name": "test_service", "service_id": MOCK_SERVICE_ID},
        ),
        patch("backend.routers.query._get_presets", return_value=fake_presets),
    ):
        resp_ok = client.get("/api/presets", headers=_Q_HEADERS)
    assert resp_ok.status_code == 200 and resp_ok.json() == fake_presets


# ── End-to-end router + repository SELECT, AST safety, schema & masking ─────


def test_query_select_executes_against_clickhouse(client, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    fake_hs_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}] if "system.one" in sql else [{"url": "/index.html", "status": 200}]
        )
    )
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)
    with patch(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
    ):
        resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT url, status FROM logs"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["columns"] == ["url", "status"] and body["row_count"] == 1
    assert body["data"][0] == {"url": "/index.html", "status": 200}
    assert isinstance(body["_debug_queries"], list) and len(body["_debug_queries"]) >= 1


@pytest.mark.parametrize(
    "unsafe_sql",
    [
        "INSERT INTO logs (url) VALUES ('/x')",
        "UPDATE logs SET status = 500",
        "DELETE FROM logs WHERE status = 200",
        "DROP TABLE logs",
        "ALTER TABLE logs ADD COLUMN extra VARCHAR",
        "ATTACH ':memory:' AS ext_db",
        "COPY logs TO '/tmp/out.parquet'",
    ],
)
def test_query_ast_safety_rejects_non_select_statements(client, unsafe_sql):
    resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": unsafe_sql})
    assert resp.status_code == 403 and resp.json()["detail"]["error"] == "sql_not_permitted"


def test_query_syntax_error_rejected_cleanly(client):
    resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT FROM WHERE ("})
    assert resp.status_code in (400, 403)
    detail = resp.json()["detail"]
    assert detail["error"] in ("sql_not_permitted", "query_failed")
    assert "parse error" in detail["message"].lower() or "syntax error" in detail["message"].lower()


def test_query_remote_analyst_masks_ips_end_to_end(client, test_service_source):
    start = datetime.datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime.datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-mask", pii_policy={"mask_ips": True}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}] if "system.one" in sql else [{"ip": "198.51.100.77", "leaked_ip": "198.51.100.88"}]
        )
    )
    console_stub = SimpleNamespace(
        client_for=lambda sid, masked=True: SimpleNamespace(
            client=console_client, tables={"request_facts": "fla_cv_view_req"}
        )
    )
    with patch("backend.high_scale.postgres_control.get_console_clients", return_value=console_stub):
        resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT ip, url AS leaked_ip FROM logs"})
    assert resp.status_code == 200, resp.text
    row = resp.json()["data"][0]
    assert row["ip"] == "[redacted]" and row["leaked_ip"] == "198.51.100.xxx"


def test_query_high_scale_validates_ast_and_masks_ips(client, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    fake_hs_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}]
            if "system.one" in sql
            else [{"client_ip": "203.0.113.9", "custom_fields['url']": "203.0.113.10"}]
        )
    )
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)
    with patch(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
    ):
        drop_resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "DROP TABLE logs"})
        assert drop_resp.status_code == 403 and drop_resp.json()["detail"]["error"] == "sql_not_permitted"

        ok_resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT count(*) FROM logs"})
        assert ok_resp.status_code == 200
        body = ok_resp.json()
        assert body["row_count"] == 1 and body["_debug_queries"][0]["engine"] == "ClickHouse"


def test_query_schema_endpoint_returns_columns_and_falls_back(client):
    with patch(
        "backend.config.get_status",
        return_value={"schema": [{"column": "timestamp", "type": "TIMESTAMPTZ"}, {"column": "url", "type": "VARCHAR"}]},
    ):
        resp = client.get(f"/api/query/schema/{MOCK_SERVICE_ID}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "test_service" and len(body["schema"]) == 2 and body["schema"][0]["column"] == "timestamp"

    with patch("backend.config.get_status", return_value={}):
        resp_fb = client.get(f"/api/query/schema/{MOCK_SERVICE_ID}")
    assert resp_fb.status_code == 200
    assert {"timestamp", "url", "status"}.issubset({c["column"] for c in resp_fb.json()["schema"]})


@pytest.mark.security_regression
def test_query_high_scale_is_closed_to_analysts_until_clickhouse_enforces_the_boundary(client, test_service_source):
    calls: list[str] = []
    fake_hs_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)
    session = SimpleNamespace(
        session_id="sess-hs", pii_policy={"mask_ips": True}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime.datetime(2026, 6, 17, 9, 0, tzinfo=UTC),
            end=datetime.datetime(2026, 6, 17, 10, 0, tzinfo=UTC),
        ),
    )
    with patch(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
    ):
        resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT count(*) FROM logs"})
    assert resp.status_code == 403 and resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert calls == []


def _analyst_on_high_scale(client, test_service_source, main_calls, console, mask_ips=True):
    main_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: main_calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=main_client)
    session = SimpleNamespace(
        session_id="sess-hs2", pii_policy={"mask_ips": mask_ips}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime.datetime(2026, 6, 17, 9, 0, tzinfo=UTC),
            end=datetime.datetime(2026, 6, 17, 10, 0, tzinfo=UTC),
        ),
    )
    with (
        patch(
            "backend.high_scale.ingest_controller.get_high_scale_service_registry",
            return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
        ),
        patch("backend.high_scale.postgres_control.get_console_clients", return_value=console),
    ):
        return client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT count(*) AS n FROM logs"})


@pytest.mark.security_regression
def test_analyst_console_runs_on_the_services_own_least_privilege_identity_through_its_views(
    client, test_service_source
):
    seen: list[tuple[str, dict]] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}] if "system.one" in sql else seen.append((sql, dict(settings or {}))) or [{"n": 7}]
        )
    )
    asked: list[bool] = []

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    main_calls: list[str] = []
    resp = _analyst_on_high_scale(client, test_service_source, main_calls, SimpleNamespace(client_for=client_for))
    assert resp.status_code == 200 and resp.json()["data"] == [{"n": 7}]
    sql, settings = seen[0]
    assert "fla_cv_view_req" in sql and "request_facts" not in sql
    scope = settings["additional_table_filters"]
    assert "event_timestamp >=" in scope and "service_id" not in scope
    assert main_calls == [] and asked == [True]


@pytest.mark.security_regression
def test_an_invite_without_mask_ips_gets_the_raw_identity(client, test_service_source):
    asked: list[bool] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in sql else [{"n": 1}]
    )

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    resp = _analyst_on_high_scale(
        client, test_service_source, [], SimpleNamespace(client_for=client_for), mask_ips=False
    )
    assert resp.status_code == 200 and asked == [False]


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_no_console_identity_is_available(client, test_service_source):
    main_calls: list[str] = []
    resp = _analyst_on_high_scale(client, test_service_source, main_calls, SimpleNamespace(client_for=lambda sid: None))
    assert resp.status_code == 403 and resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert main_calls == []


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_the_identity_fails_its_isolation_check(client, test_service_source):
    def refuse(sid):
        raise RuntimeError("console identity failed the row isolation check")

    main_calls: list[str] = []
    resp = _analyst_on_high_scale(client, test_service_source, main_calls, SimpleNamespace(client_for=refuse))
    assert resp.status_code == 403 and resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert main_calls == []


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("policy", "expected_masked"),
    [
        (None, True),
        ({}, True),
        ({"mask_ips": None}, True),
        ({"mask_ips": "false"}, True),
        ({"mask_ips": True}, True),
        ({"mask_ips": False}, False),
    ],
)
def test_the_console_picks_the_masked_identity_unless_the_invite_explicitly_disables_masking(
    client, test_service_source, policy, expected_masked
):
    asked: list[bool] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in sql else [{"n": 1}]
    )

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    session = SimpleNamespace(session_id="s", pii_policy=policy, service_ids=[test_service_source["service_id"]])
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime.datetime(2026, 6, 17, 9, 0, tzinfo=UTC),
            end=datetime.datetime(2026, 6, 17, 10, 0, tzinfo=UTC),
        ),
    )
    hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=lambda *a, **k: []))
    with (
        patch(
            "backend.high_scale.ingest_controller.get_high_scale_service_registry",
            return_value=SimpleNamespace(resolve=lambda sid: hs_service),
        ),
        patch(
            "backend.high_scale.postgres_control.get_console_clients",
            return_value=SimpleNamespace(client_for=client_for),
        ),
    ):
        resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT count(*) AS n FROM logs"})
    assert resp.status_code == 200 and asked == [expected_masked]


# ── Merged from tests/routers/test_rum_router.py ──

SVC = "svc-rum-admin-test"
_TOKEN_SERVICE_ID = "svc-rum-token"
WINDOW = "start_time=2026-08-05T00:00:00Z&end_time=2026-08-05T23:59:59Z"
ANCHOR = "2026-08-05T12:00:30Z"
EMPTY = {"is_mock": False, "no_data": True, "beacon_count": 0}

client = TestClient(app)


def _parse_sse(text: str) -> list[dict[str, Any]]:
    return [
        json.loads(line[len("data:") :].strip())
        for raw_line in text.splitlines()
        if (line := raw_line.strip()).startswith("data:") and line[len("data:") :].strip()
    ]


def _make_app(*, analyst: bool = False) -> FastAPI:
    test_app = FastAPI()
    if analyst:

        @test_app.middleware("http")
        async def _inject_analyst(request: Request, call_next):
            request.state.analyst_session = MagicMock(session_id="sess-analyst-rum")
            return await call_next(request)

    test_app.include_router(_rum_api_router)
    return test_app


def _fake_request(analyst_session: object | None) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


@pytest.fixture
def _reset_faro_cache():
    from backend.core import faro_versions

    faro_versions.reset_versions_cache()
    yield
    faro_versions.reset_versions_cache()


# ── Section 1: RUM Status & Analyst Projection ───────────────────────────────


def _rum_settings(
    vitals: bool = True, perf: bool = False, errs: bool = True, evts: bool = False, cond: str = ""
) -> dict[str, Any]:
    return {
        "capture_vitals": vitals,
        "capture_performance": perf,
        "capture_errors": errs,
        "capture_events": evts,
        "custom_condition": cond,
    }


def _status_cfg(sha: str | None, rum_extra: dict[str, Any] | None = None) -> dict[str, Any]:
    base: dict[str, Any] = {"rum_enabled": True, "rum_enabled_at": "2026-10-18T08:00:00Z"}
    if sha is not None:
        base["rum_vcl_sha"] = sha
    if rum_extra is not None:
        base["rum"] = {"enabled": True, **rum_extra}
    return base


_FULL_RUM_CFG = {
    "enabled_at": "2026-10-18T08:00:00Z",
    "deployed_vcl_sha": "sha-current",
    **_rum_settings(cond='req.url.ext != "jpg"'),
}


@pytest.mark.parametrize(
    ("cfg", "cur_sha", "expected_drift", "expected_deployed", "expect_save"),
    [
        (_status_cfg("sha-current", _FULL_RUM_CFG), "sha-current", False, "sha-current", False),
        (_status_cfg("sha-old-deployed", {}), "sha-new-template", True, "sha-old-deployed", False),
        (_status_cfg(None), "freshsha123", False, "freshsha123", False),
    ],
)
def test_admin_rum_status_variants(cfg, cur_sha, expected_drift, expected_deployed, expect_save):
    test_client = TestClient(_make_app(analyst=False))
    mock_save = MagicMock()
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save)),
        patch("backend.routers.rum.rum_vcl_fingerprint", return_value=cur_sha),
    ):
        res = test_client.get(f"/api/services/{SVC}/rum/status")

    assert res.status_code == 200
    body = res.json()
    assert body["enabled"] is True and body["vcl_drift"] is expected_drift
    assert body["deployed_vcl_sha"] == expected_deployed and body["current_vcl_sha"] == cur_sha
    assert mock_save.called is expect_save
    if "custom_condition" in cfg.get("rum", {}):
        assert body["custom_condition"] == 'req.url.ext != "jpg"' and body["capture_performance"] is False


@pytest.mark.security_regression
async def test_analyst_status_excludes_vcl_fingerprint_fields(with_config, monkeypatch):
    svc = "svc-rum-status-projection"
    with_config[svc] = {"rum_enabled": True, "rum_enabled_at": "2026-01-01T00:00:00Z", "rum_vcl_sha": "deadbeef"}
    monkeypatch.setattr(
        rum_router,
        "rum_vcl_fingerprint",
        lambda service_id: pytest.fail("rum_vcl_fingerprint must not be called for an analyst caller"),
    )
    sess = SimpleNamespace(session_id="s-test", service_ids=[svc], email="analyst@example.com")
    result = await rum_router.rum_status(_fake_request(sess), service_id=svc)
    assert result == {"enabled": True, "enabled_at": "2026-01-01T00:00:00Z"}
    for forbidden in ("deployed_vcl_sha", "current_vcl_sha", "vcl_drift"):
        assert forbidden not in result


@pytest.mark.security_regression
async def test_analyst_can_read_enabled_field(with_config):
    svc = "svc-rum-status-projection"
    with_config[svc] = {"rum_enabled": False}
    sess = SimpleNamespace(session_id="s-test", service_ids=[svc], email="analyst@example.com")
    result = await rum_router.rum_status(_fake_request(sess), service_id=svc)
    assert result["enabled"] is False and "enabled_at" in result


# ── Section 2: RUM Settings, Versions, Enable/Disable/Upgrade & RBAC ─────────


@pytest.mark.parametrize(("custom_condition", "expect_reconcile"), [("", False), ('req.url !~ "^/healthz"', True)])
def test_update_settings_reconcile_and_missing(custom_condition: str, expect_reconcile: bool):
    test_client = TestClient(_make_app(analyst=False))
    with patch("backend.config.load_config", return_value=None):
        assert test_client.post(f"/api/services/{SVC}/rum/settings", json={"capture_vitals": True}).status_code == 404

    cfg = {
        "fastly_api_key": "tok-stored",
        "rum_enabled": True,
        "rum": {"enabled": True, **_rum_settings(True, True, True, True)},
    }
    payload = _rum_settings(cond=custom_condition)
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
        patch("backend.provision.rum_assets.upload_rum_tracker_js", return_value={"skipped": False}) as mock_upload,
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile,
    ):
        res = test_client.post(f"/api/services/{SVC}/rum/settings", json=payload)
    assert res.status_code == 200 and res.json()["ok"] is True
    mock_save.assert_called_once()
    mock_upload.assert_called_once_with(SVC, "tok-stored", overwrite=True)
    assert mock_reconcile.called is expect_reconcile


@pytest.mark.parametrize(
    ("upload_err", "reconcile_err", "custom_condition", "expected_error_code"),
    [
        (RuntimeError("FOS bucket unreachable"), None, "", "rum_tracker_upload_failed"),
        (None, RuntimeError("Fastly VCL compile error"), 'req.http.X-Test == "1"', "rum_reconcile_failed"),
    ],
)
def test_update_settings_rolls_back_on_failure(upload_err, reconcile_err, custom_condition, expected_error_code):
    cfg = {
        "fastly_api_key": "tok-stored",
        "rum_enabled": True,
        "rum": {"enabled": True, "capture_vitals": True, "custom_condition": ""},
    }
    test_client = TestClient(_make_app(analyst=False))
    payload = _rum_settings(False, False, False, False, cond=custom_condition)
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
        patch(
            "backend.provision.rum_assets.upload_rum_tracker_js",
            side_effect=upload_err if upload_err else (lambda *a, **kw: {"skipped": False}),
        ),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state", side_effect=reconcile_err),
    ):
        res = test_client.post(f"/api/services/{SVC}/rum/settings", json=payload)
    assert res.status_code == 500 and res.json()["detail"]["error"] == expected_error_code
    assert mock_save.call_count == 2
    rollback_cfg = mock_save.call_args_list[1].args[1]
    assert rollback_cfg["rum"]["capture_vitals"] is True and rollback_cfg["rum"]["custom_condition"] == ""


@pytest.mark.usefixtures("_reset_faro_cache")
@pytest.mark.parametrize(
    ("faro_ver", "fetched", "exp_avail", "exp_latest", "exp_upd"),
    [
        ("2.8.2", ["2.9.0", "2.8.2", "2.8.1"], ["2.9.0", "2.8.2", "2.8.1"], "2.9.0", True),
        ("2.9.0", ["2.9.0", "2.8.2"], ["2.9.0", "2.8.2"], "2.9.0", False),
        (None, ["2.9.0", "2.8.2"], ["2.9.0", "2.8.2"], "2.9.0", False),
        ("2.8.2", ValueError("registry 503"), [], None, False),
    ],
)
def test_rum_versions_variants(with_config, monkeypatch, faro_ver, fetched, exp_avail, exp_latest, exp_upd):
    svc = "TestRumVersionsUpgradeSvc"
    with_config[svc] = {"service_id": svc, **({"rum": {"faro_version": faro_ver}} if faro_ver else {})}

    async def fake_fetch():
        if isinstance(fetched, Exception):
            raise fetched
        return fetched

    monkeypatch.setattr("backend.core.faro_versions.fetch_available_faro_versions", fake_fetch)
    r = client.get(f"/api/services/{svc}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] == exp_avail and body["current"] == faro_ver
    assert body["latest"] == exp_latest and body["update_available"] is exp_upd


@pytest.mark.parametrize(
    ("action", "activate", "final_enabled", "expected_msg_substr"),
    [
        ("enable", True, True, "enabled successfully"),
        ("enable", False, False, "draft configuration compiled"),
        ("disable", True, False, "disabled successfully"),
        ("disable", False, False, "draft configuration compiled"),
    ],
)
def test_enable_disable_sse_modes(
    with_config, monkeypatch, action: str, activate: bool, final_enabled: bool, expected_msg_substr: str
):
    svc = "TestRumEnableDisableSvc"
    with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN", "rum_enabled": not final_enabled}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": f"{action}ing"}
        with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN", "rum_enabled": final_enabled}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: SimpleNamespace(reload=lambda: None))

    r = client.post(f"/api/services/{svc}/rum/{action}", json={"activate": activate})
    assert r.status_code == 200
    done = next(e for e in _parse_sse(r.text) if e["type"] == "done")
    assert done["rum"]["enabled"] is final_enabled and expected_msg_substr in done["message"]


@pytest.mark.parametrize("action", ["enable", "disable"])
def test_enable_disable_errors_and_resilience(with_config, monkeypatch, action: str):
    svc = "TestRumEnableDisableSvc"
    assert client.post(f"/api/services/{svc}/rum/{action}", json={}).status_code == 404
    with_config[svc] = {"service_id": svc, "fastly_api_key": ""}
    assert client.post(f"/api/services/{svc}/rum/{action}", json={}).status_code == 400

    monkeypatch.setattr(
        "backend.routers.rum.run_with_events", lambda f, *a, **kw: iter([{"type": "status", "message": "ok"}])
    )
    monkeypatch.setattr(
        "backend.cron.scheduler.get_scheduler", lambda: (_ for _ in ()).throw(RuntimeError("scheduler down"))
    )
    r = client.post(f"/api/services/{svc}/rum/{action}", json={"token": "SUPPLIED-TOKEN"})
    assert r.status_code == 200 and any(e["type"] == "done" for e in _parse_sse(r.text))
    if action == "enable":
        assert with_config[svc]["fastly_api_key"] == "SUPPLIED-TOKEN"

    with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN"}

    def _boom_events(func, *args, **kwargs):
        yield {"type": "status", "message": "starting"}
        raise RuntimeError(f"{action} orchestrator boom")

    monkeypatch.setattr("backend.routers.rum.run_with_events", _boom_events)
    events = _parse_sse(client.post(f"/api/services/{svc}/rum/{action}", json={}).text)
    assert any(e["type"] == "error" and f"{action} orchestrator boom" in e["message"] for e in events)
    assert not any(e["type"] == "done" for e in events)


@pytest.mark.usefixtures("_reset_faro_cache")
def test_upgrade_validation_and_sse_lifecycle(with_config, monkeypatch):
    svc = "TestRumVersionsUpgradeSvc"
    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", AsyncMock(return_value=["2.9.0", "2.8.2"]))

    with_config[svc] = {"service_id": svc, "fastly_api_key": ""}
    r_notok = client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "2.9.0"})
    assert r_notok.status_code == 400 and "api key" in r_notok.json()["detail"]["error"].lower()

    with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN"}
    mock_run = MagicMock()
    monkeypatch.setattr("backend.routers.rum.run_with_events", mock_run)
    r_unk = client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "9.9.9"})
    assert r_unk.status_code == 400 and r_unk.json()["detail"]["error"] == "unknown_faro_version"
    mock_run.assert_not_called()

    captured: dict[str, Any] = {}

    def _ok_events(func, *args, **kwargs):
        captured.update(func=func, args=args, kwargs=kwargs)
        yield {"type": "status", "message": "downloading v2.9.0"}
        with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN", "rum": {"faro_version": "2.9.0"}}

    monkeypatch.setattr("backend.routers.rum.run_with_events", _ok_events)
    r_ok = client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "2.9.0"})
    assert r_ok.status_code == 200
    done = next(e for e in _parse_sse(r_ok.text) if e["type"] == "done")
    assert done["rum"]["faro_version"] == "2.9.0"
    assert captured["func"] is upgrade_faro_version and captured["args"] == (svc, "2.9.0", "TOKEN")
    assert captured["kwargs"]["activate"] is True

    def _err_events(func, *args, **kwargs):
        yield {"type": "status", "message": "downloading"}
        raise RuntimeError("upload failed: FOS 503")

    monkeypatch.setattr("backend.routers.rum.run_with_events", _err_events)
    events = _parse_sse(client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "2.9.0"}).text)
    assert any(e["type"] == "error" and "fos" in e["message"].lower() for e in events)
    assert not any(e["type"] == "done" for e in events)


class TestRumAdminRbacEnforcement:
    """Analyst Path B sessions must receive 403 {"error": "admin_only"} on all RUM admin/mutation endpoints."""

    @pytest.mark.parametrize(
        ("method", "suffix", "payload"),
        [
            ("POST", "enable", {"activate": True}),
            ("POST", "disable", {"activate": True}),
            ("GET", "versions", None),
            ("POST", "upgrade", {"version": "2.3.1", "activate": True}),
            ("POST", "settings", {"capture_vitals": True}),
        ],
    )
    def test_analyst_blocked_at_middleware_and_route_dependency(
        self, method: str, suffix: str, payload: dict[str, Any] | None
    ):
        path = f"/api/services/{SVC}/rum/{suffix}"
        assert _is_blocked_path(path) is True
        test_client = TestClient(_make_app(analyst=True))
        res = test_client.get(path) if method == "GET" else test_client.post(path, json=payload)
        assert res.status_code == 403 and res.json()["detail"]["error"] == "admin_only"


# ── Section 3: RUM Analytics, Vitals, Filters, Beacon Health & Goldens ───────


def test_rum_analytics_fallback_and_live_events(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.core.fos_client.get_source_for_service",
        lambda sid, **kw: {"name": sid, "service_id": sid},
    )

    def _empty_exec(sql: str, params: dict | None = None, settings: dict | None = None):
        if "rum_analytics_counts" in sql:
            return [{"beacons": 0, "pageviews": 0, "interactions": 0, "errors": 0}]
        if "rum_analytics_any_data" in sql:
            return [{"has_data": 0}]
        return []

    svc_empty = SimpleNamespace(
        service_id="svc-test-rum-fallback",
        client=SimpleNamespace(execute=_empty_exec),
        watermark=lambda: None,
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda sid: svc_empty),
    )

    response = client.get("/api/services/svc-test-rum-fallback/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["no_data"] is True and data["beacon_count"] == 0
    assert data["worst_pages"] == [] and data["errors"] == [] and data["trends"]["lcp"] == []
    assert client.get("/api/services/svc-test-rum-fallback/rum/live-events").json() == []

    now = datetime.datetime.now(datetime.UTC)
    live_rows = [
        {
            "timestamp": now,
            "type": "pageview",
            "pathname": "/live-test",
            "metric_name": m,
            "metric_value": v,
            "metric_rating": "good",
            "browser": "Chrome",
            "os": "macOS",
            "device": "Desktop",
            "cid": "cid1",
            "req_id": "req1",
            "error_message": "",
            "city": "Austin",
            "region": "TX",
            "country": "US",
            "pop": "DFW",
            "tls": "TLSv1.3",
            "ttfb": 30.0,
        }
        for m, v in (("load_time", 1.5), ("lcp", 1.7), ("cls", 0.01), ("inp", 100.0))
    ]
    svc_live = SimpleNamespace(
        service_id="svc-test-rum-live",
        client=SimpleNamespace(
            execute=lambda sql, params=None, settings=None: live_rows if "rum_live_events" in sql else []
        ),
        watermark=lambda: None,
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda sid: svc_live),
    )
    events = client.get("/api/services/svc-test-rum-live/rum/live-events").json()
    assert len(events) == 4 and events[0]["path"] == "/live-test"
    assert events[0]["browser"] == "Chrome" and events[0]["os"] == "macOS"


@pytest.mark.parametrize(
    ("mode", "browser_val", "expected_browsers", "expected_count"),
    [
        ("include", "Chrome", {"Chrome": 8}, 8),
        ("include", "Safari", {"Safari": 7}, 7),
        ("include", "Firefox", {}, 0),
        ("exclude", "Chrome", {"Safari": 7}, 7),
    ],
)
def test_rum_analytics_with_filters(mode: str, browser_val: str, expected_browsers: dict, expected_count: int) -> None:
    service_id = f"test_service_with_filters_{mode}_{browser_val}"
    captured_filters: list[dict] = []

    def _fake_analytics(svc, start_time, end_time, filters=None):
        captured_filters.append(filters or {})
        return {
            "is_mock": False,
            "no_data": expected_count == 0,
            "beacon_count": expected_count,
            "pageview_count": expected_count,
            "interaction_count": 0,
            "error_count": 0,
            "vitals": {
                **{
                    m: {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}}
                    for m in ("lcp", "cls", "inp")
                },
                "fid": {"p75": None, "fcp": None, "ttfb": None},
            },
            "worst_pages": [],
            "errors": [],
            "trends": {
                k: [] for k in ("timestamps", "lcp", "cls", "error_rate", "pageviews", "interactions", "errors")
            },
            "environments": {"browsers": expected_browsers, "os": {}, "devices": {}},
        }

    filters = json.dumps({"browser": {"mode": mode, "values": [browser_val]}})
    with (
        patch("backend.core.fos_client.get_source_for_service", lambda sid, **kw: {"name": sid, "service_id": sid}),
        patch("backend.high_scale.rum.rum_analytics", side_effect=_fake_analytics),
    ):
        response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["beacon_count"] == expected_count
    assert captured_filters == [{"browser": {"mode": mode, "values": [browser_val]}}]
    if expected_browsers:
        assert data["environments"]["browsers"] == expected_browsers


@pytest.mark.security_regression
def test_analyst_request_outside_invite_window_is_clamped() -> None:
    service_id = "test_rum_analytics_clamp_analyst"
    src = {"name": service_id, "service_id": service_id}
    analyst_sess = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    analyst_req = _fake_request(analyst_sess)
    analyst_ctx = make_test_request_context(
        service_id=service_id,
        source=src,
        session=analyst_sess,
        read_only=True,
        time_bounds=get_analyst_time_bounds(typing.cast(typing.Any, analyst_req)),
    )

    def _fake_analytics(svc, start_time, end_time, filters=None):
        start_dt = parse_iso_utc(start_time)
        pages = [
            {"path": "/just-now", "views": 1, "avg_load_time": 1.5, "lcp_p75": 1.5, "cls_p75": 0.01, "error_rate": 0.0}
        ]
        if start_dt is not None and start_dt.year == 2000:
            pages.append(
                {
                    "path": "/thirty-days-old",
                    "views": 1,
                    "avg_load_time": 1.5,
                    "lcp_p75": 1.5,
                    "cls_p75": 0.01,
                    "error_rate": 0.0,
                }
            )
        return {
            "is_mock": False,
            "no_data": False,
            "beacon_count": len(pages),
            "worst_pages": pages,
            "errors": [],
            "trends": {"lcp": []},
        }

    with patch("backend.high_scale.rum.rum_analytics", side_effect=_fake_analytics):
        result = asyncio.run(
            rum_router.rum_analytics(
                request=typing.cast(typing.Any, analyst_req),
                start_time="2000-01-01T00:00:00Z",
                end_time=None,
                filters=None,
                ctx=analyst_ctx,
            )
        )
        assert "/thirty-days-old" not in [p["path"] for p in result["worst_pages"]]

        admin_ctx = make_test_request_context(
            service_id=service_id, source=src, session=None, read_only=True, time_bounds=TimeBounds(None, None)
        )
        admin_result = asyncio.run(
            rum_router.rum_analytics(
                request=typing.cast(typing.Any, _fake_request(None)),
                start_time="2000-01-01T00:00:00Z",
                end_time=None,
                filters=None,
                ctx=admin_ctx,
            )
        )
        assert "/thirty-days-old" in [p["path"] for p in admin_result["worst_pages"]]


# ── Section 4: RUM JS Assets & Beacon Health ─────────────────────────────────


def test_get_rum_tracker_and_faro_sdk_js(with_config, monkeypatch):
    svc = "TestRumAssetsSvc"
    with_config[svc] = {"service_id": svc, "rum": {"faro_version": "2.9.0"}}
    fake_js = "console.log('rum tracker code');"
    monkeypatch.setattr("backend.provision.rum_assets.generate_rum_tracker_js", lambda sid: f"{fake_js} /* {sid} */")

    for url, expected_sid in [("/js/rum.js", svc), ("/js/rum.js?service_id=CustomSvc", svc)]:
        r = client.get(url)
        assert r.status_code == 200 and r.headers["content-type"].startswith("application/javascript")
        assert f"{fake_js} /* {expected_sid} */" in r.text

    with_config["CustomSvc"] = {"service_id": "CustomSvc"}
    assert f"{fake_js} /* CustomSvc */" in client.get("/js/rum.js?service_id=CustomSvc").text

    async def mock_fetch(version):
        return f"console.log('faro bundle version {version}');".encode()

    monkeypatch.setattr("backend.core.faro_versions.fetch_faro_bundle", mock_fetch)
    r_sdk = client.get("/js/faro-sdk.js")
    assert r_sdk.status_code == 200 and "console.log('faro bundle version 2.9.0');" in r_sdk.text


@pytest.mark.security_regression
def test_get_rum_tracker_js_error_does_not_leak_exception_text(with_config, monkeypatch):
    svc = "TestRumAssetsSvc"
    with_config[svc] = {"service_id": svc}
    secret = '*/ </script> alert("x"); \n /etc/secret-path "quoted" \'single\''
    monkeypatch.setattr(
        "backend.provision.rum_assets.generate_rum_tracker_js", lambda sid: (_ for _ in ()).throw(RuntimeError(secret))
    )
    r = client.get("/js/rum.js")
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/javascript")
    for forbidden in ("secret-path", "</script>", "alert", "RuntimeError"):
        assert forbidden not in r.text
    assert "\n" not in r.text.strip() and r.text.count("*/") == 1


@pytest.mark.parametrize(
    ("mode", "expected_recent", "expected_complete", "expected_msg_substr"),
    [
        ("recent", 1, True, "RUM is working"),
        ("stale", 0, False, "No beacons received"),
        ("empty", 0, False, "No beacons received"),
        ("no_high_scale", 0, False, "Waiting for beacons..."),
    ],
)
def test_beacon_health_states(
    monkeypatch, mode: str, expected_recent: int, expected_complete: bool, expected_msg_substr: str
):
    monkeypatch.setattr(
        "backend.config.load_config", lambda sid: {"service_id": sid, "rum": {"enabled": True}, "rum_enabled": True}
    )
    now = datetime.datetime.now(datetime.UTC)
    if mode == "no_high_scale":
        monkeypatch.setattr(
            "backend.high_scale.ingest_controller.get_high_scale_service_registry",
            lambda: SimpleNamespace(resolve=lambda sid: None),
        )
    else:
        delta = datetime.timedelta(minutes=5) if mode == "recent" else datetime.timedelta(hours=3)
        c_val = 1 if mode in ("recent", "stale") else 0
        last_ts = (now - delta) if c_val > 0 else None
        svc = SimpleNamespace(
            service_id=f"test_beacon_health_{mode}",
            client=SimpleNamespace(execute=lambda sql, params=None, settings=None: [{"c": c_val, "last_ts": last_ts}]),
            watermark=lambda: None,
        )
        monkeypatch.setattr(
            "backend.high_scale.ingest_controller.get_high_scale_service_registry",
            lambda: SimpleNamespace(resolve=lambda sid: svc),
        )

    r = client.get(f"/api/services/test_beacon_health_{mode}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True and data["recent_beacons"] == expected_recent
    assert data["setup_complete"] is expected_complete and expected_msg_substr in data["message"]


# ── Section 5: Token Window Resolution ───────────────────────────────────────


@pytest.fixture
def captured():
    calls: list[dict] = []

    def fake(svc, start_time, end_time, filters=None):
        calls.append(
            {
                "start_time": start_time.replace("+00:00", "Z") if isinstance(start_time, str) else start_time,
                "end_time": end_time.replace("+00:00", "Z") if isinstance(end_time, str) else end_time,
                "filters": filters,
            }
        )
        return EMPTY

    with (
        patch("backend.core.fos_client.get_source_for_service", lambda sid, **kw: {"name": sid, "service_id": sid}),
        patch("backend.high_scale.rum.rum_analytics", fake),
    ):
        yield calls


_WIN_FULL = ("2026-08-05T00:00:00Z", "2026-08-05T23:59:59Z")
_WIN_1H = ("2026-08-05T00:00:00Z", "2026-08-05T01:00:00Z")


@pytest.mark.parametrize(
    ("params", "expected_bounds"),
    [
        ({"range_token": "24h", "anchor": ANCHOR}, resolve_window("24h", ANCHOR)),
        (
            {
                "range_token": "1h",
                "anchor": ANCHOR,
                "start_time": "2000-01-01T00:00:00Z",
                "end_time": "2000-01-02T00:00:00Z",
            },
            resolve_window("1h", ANCHOR),
        ),
        ({"start_time": _WIN_FULL[0], "end_time": _WIN_FULL[1], "anchor": ANCHOR}, _WIN_FULL),
        ({"range_token": "bogus", "anchor": ANCHOR, "start_time": _WIN_1H[0], "end_time": _WIN_1H[1]}, _WIN_1H),
    ],
)
def test_token_window_resolution(captured, params: dict, expected_bounds: tuple[str, str]) -> None:
    r = client.get(f"/api/services/{_TOKEN_SERVICE_ID}/rum/analytics", params=params)
    assert r.status_code == 200
    assert (captured[0]["start_time"], captured[0]["end_time"]) == expected_bounds


@pytest.mark.security_regression
def test_analyst_token_window_is_clamped_to_invite(captured) -> None:
    session = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    req = _fake_request(session)
    bounds = get_analyst_time_bounds(typing.cast(typing.Any, req))
    source = {"name": _TOKEN_SERVICE_ID, "service_id": _TOKEN_SERVICE_ID}

    def _run(sess, tb, request_obj) -> None:
        ctx: RequestContext = make_test_request_context(
            service_id=_TOKEN_SERVICE_ID, source=source, session=sess, read_only=True, time_bounds=tb
        )
        asyncio.run(
            rum_router.rum_analytics(
                request=typing.cast(typing.Any, request_obj),
                start_time=None,
                end_time=None,
                filters=None,
                ctx=ctx,
                range_token="30d",
                anchor=None,
            )
        )

    _run(session, bounds, req)
    _run(None, TimeBounds(None, None), _fake_request(None))

    def span_h(c: dict) -> float:
        s, e = parse_iso_utc(c["start_time"]), parse_iso_utc(c["end_time"])
        assert s is not None and e is not None
        return (e - s).total_seconds() / 3600

    assert span_h(captured[0]) <= 1.01
    assert span_h(captured[1]) > 24 * 29


# ── Section 6: Web Vitals, Page Telemetry & UX Events ────────────────────────


def _vital(id_: str, name: str = "LCP", value: float = 1850.5, rating: str = "good", **kw) -> dict[str, Any]:
    return {"id": id_, "name": name, "value": value, "rating": rating, **kw}


@pytest.mark.parametrize(
    ("payload", "expected_status"),
    [
        (_vital("v1-1234", pathname="/dashboard", navigation_type="navigate", delta=1850.5), 200),
        (_vital("v1-minimal", "TTFB", 220), 200),
        (_vital("v1-bad", "NOT_A_REAL_METRIC", 0), 422),
        (_vital("v1-bad-rating", "CLS", 0.05, "amazing"), 422),
    ],
)
def test_web_vitals_payload_validation(payload: dict, expected_status: int):
    r = client.post("/api/web-vitals", json=payload)
    assert r.status_code == expected_status
    if expected_status == 200:
        assert r.json().get("ok") is True


def test_web_vitals_collection_and_page_telemetry(monkeypatch, tmp_path, caplog):
    from backend import config as svcconfig
    from backend.core.metadata import usage_log

    sink = tmp_path / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)
    monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    r_off = client.post("/api/web-vitals", json=_vital("off-1", value=1200, pathname="/dashboard"))
    assert r_off.status_code == 200 and not sink.exists()

    monkeypatch.setenv("WEB_VITALS_COLLECT", "1")
    r_on = client.post(
        "/api/web-vitals",
        json=_vital("on-1", value=2412.0, pathname="/dashboard", navigation_type="navigate", delta=2412.0),
    )
    assert r_on.status_code == 200
    rec = json.loads(sink.read_text(encoding="utf-8").strip().splitlines()[0])
    assert rec["name"] == "LCP" and rec["value"] == 2412.0 and rec["cohort"] == "admin" and rec["ts"].endswith("Z")

    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "demo_service")
    usage_log.log_telemetry_query("demo_service", "page-456", "req-789", "clickhouse", "SELECT 42", 12.3)

    res_pt = client.get("/api/debug/page-telemetry?page_load_id=page-456")
    assert res_pt.status_code == 200 and res_pt.json()["queries"][0]["sql_query"] == "SELECT 42"

    for payload, expected_status in [
        (
            {
                "event": "column_reordered",
                "pathname": "/sessions",
                "component_id": "Live Sessions",
                "details": {"column_id": "ja4", "from_index": 4, "to_index": 1, "column_count": 9},
            },
            200,
        ),
        ({"event": "filter_cleared"}, 200),
        ({"pathname": "/dashboard"}, 422),
        ({"event": "column_reordered", "typo_field": "ignored"}, 422),
        ({"event": "x" * 81}, 422),
    ]:
        assert client.post("/api/ux-events", json=payload).status_code == expected_status

    with caplog.at_level(logging.INFO, logger="backend.ux_events"):
        r_log = client.post("/api/ux-events", json={"event": "column_reordered"})
    assert r_log.status_code == 200
    matched = [r for r in caplog.records if r.message == "ux_event"]
    assert (
        matched
        and getattr(matched[-1], "ux_cohort", None) == "admin"
        and getattr(matched[-1], "ux_event", None) == "column_reordered"
    )
