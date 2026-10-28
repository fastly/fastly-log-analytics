"""Tests for ``backend.routers.dashboard``, ``query``, and ``rum``."""

from __future__ import annotations

import asyncio
import datetime
import inspect
import json
import logging
import os
import threading
import time as _time
import typing
from datetime import UTC
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pandas as pd
import pytest
import time_machine
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

import backend.high_scale.dashboard as hsd
import backend.high_scale.ingest_controller as hsreg
import backend.routers.dashboard as dash
from backend.core import request_telemetry as web_vitals_store
from backend.core.duckdb_pool import _PoolBusy
from backend.core.request_context import RequestContext, build_request_context
from backend.main import app
from backend.models.analytics import AggregatesRequest, QueryRequest
from backend.provision.rum_orchestrator_v2 import upgrade_faro_version
from backend.repositories import security as security_repo
from backend.routers import rum as rum_router
from backend.routers.query import _redact_paths
from backend.routers.rum import router as _rum_api_router
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.remote_access import TimeBounds, _is_blocked_path, get_analyst_time_bounds
from backend.utils.time_window import resolve_window
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.rum import insert_rum_beacons as _insert_beacons
from tests._helpers.rum import setup_temp_rum_db
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


@pytest.fixture
def forbidden_clickhouse(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_ENABLED", "true")
    adapter = MagicMock(side_effect=AssertionError("archived evaluator reached from dashboard"))
    ch_client = MagicMock(side_effect=AssertionError("ClickHouse client reached from dashboard"))
    monkeypatch.setattr("backend.core.clickhouse.get_clickhouse_client", ch_client)
    monkeypatch.setattr("backend.core.clickhouse.ClickHouseClient.execute", ch_client)
    yield adapter, ch_client
    adapter.assert_not_called()
    ch_client.assert_not_called()


@pytest.mark.parametrize("endpoint", ["aggregates", "bundle"])
@pytest.mark.parametrize("sections", [None, ["core", "topten"], ["topten"]])
def test_dashboard_stays_ducklake_when_clickhouse_enabled(
    client, endpoint, sections, stub_aggregates, stub_top_bots, forbidden_clickhouse
):
    body = {
        "fields": ["country"],
        "filters": {},
        "chart_interval": "1 hour",
        "chart_metric": "requests",
        "start_time": "2026-09-01T00:00:00Z",
        "end_time": "2026-09-02T00:00:00Z",
        **({"sections": sections} if sections is not None else {}),
    }
    resp = client.post(f"/api/dashboard/{endpoint}", json=body)
    assert resp.status_code == 200, resp.text
    stub_aggregates.assert_called_once()
    args = stub_aggregates.call_args.kwargs
    assert "sections" not in args and args["include_top_n"] is True and args["fields_filter"] == ["country"]
    for flag in ("include_time_series", "include_conn_requests", "include_map_data"):
        assert args[flag] is (sections != ["topten"])
    if endpoint == "bundle" and sections is None:
        stub_top_bots.assert_called_once()
        assert resp.json()["top_bots"]["bots"] == [{"name": "Googlebot", "request_count": 42}]
    else:
        stub_top_bots.assert_not_called()


@pytest.mark.parametrize("endpoint", ["aggregates", "bundle"])
def test_ducklake_receives_analyst_clamp_when_clickhouse_enabled(
    client, monkeypatch, endpoint, stub_aggregates, in_memory_duckdb, test_service_source, forbidden_clickhouse
):
    start = datetime.datetime(2026, 9, 1, 6, tzinfo=UTC)
    end = datetime.datetime(2026, 9, 1, 8, tzinfo=UTC)
    ctx = make_test_request_context(
        con=in_memory_duckdb,
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
    args = stub_aggregates.call_args.kwargs
    assert datetime.datetime.fromisoformat(args["start_time"].replace("Z", "+00:00")) == start
    assert datetime.datetime.fromisoformat(args["end_time"].replace("Z", "+00:00")) == end
    assert args["src"] is test_service_source and args["con"] is in_memory_duckdb and "sections" not in args


def test_invalid_filter_rejected_before_dispatch(client, monkeypatch, forbidden_clickhouse):
    dispatch = MagicMock(side_effect=AssertionError("invalid request dispatched"))
    monkeypatch.setattr("backend.repositories.dashboard.get_aggregates", dispatch)
    resp = client.post(
        "/api/dashboard/aggregates", json={"filters": {"country": {"mode": "invalid", "values": ["US"]}}}
    )
    assert resp.status_code == 422
    dispatch.assert_not_called()


@pytest.fixture
def stub_aggregates(monkeypatch):
    stub = MagicMock(
        return_value={
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
    monkeypatch.setattr("backend.repositories.dashboard.get_aggregates", stub)
    return stub


@pytest.fixture
def stub_top_bots(monkeypatch):
    stub = MagicMock(
        return_value={
            "bots": [{"name": "Googlebot", "request_count": 42}],
            "ngwaf_bots": [],
            "section_timings": [{"section": "bots:query", "time_ms": 3.0}],
            "debug_queries": [{"sql": "SELECT bot", "time_ms": 0.5, "rows": 1, "caller": "bots_caller"}],
            "debug_calls": [],
        }
    )
    monkeypatch.setattr("backend.repositories.security.get_top_bots", stub)
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
    assert "bundle:aggregates" in sections and "bundle:top_bots" in sections
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
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)
    resp = client.post("/api/dashboard/raw/csv", json=_CSV_BODY)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    if row_count == 0:
        assert resp.text == ""
    else:
        assert "attachment" in resp.headers.get("content-disposition", "")
        assert resp.text.count("timestamp,status") == 1
        assert len([ln for ln in resp.text.split("\n") if ln]) == row_count + 1


def _inject_mask_ips_analyst(in_memory_duckdb, test_service_source):
    session = SimpleNamespace(service_ids=[test_service_source["service_id"]], pii_policy={"mask_ips": True})
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source, _con_override=in_memory_duckdb, session=session
    )


@pytest.mark.security_regression
def test_raw_csv_masks_ip_and_redacts_cookie_session_for_masking_analyst(
    client, monkeypatch, in_memory_duckdb, test_service_source
):
    df = pd.DataFrame(
        {"ip": ["203.0.113.45", "198.51.100.7"], "cookie_session": ["a1b2c3d4deadbeef", ""], "url": ["/a", "/b"]}
    )
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)
    _inject_mask_ips_analyst(in_memory_duckdb, test_service_source)

    resp = client.post("/api/dashboard/raw/csv", json=_CSV_BODY)
    assert resp.status_code == 200, resp.text
    text = resp.text
    assert "deadbeef" not in text and "203.0.113.45" not in text
    assert "[redacted]" in text and "203.0.113.xxx" in text
    assert "/a" in text and "/b" in text


@pytest.mark.security_regression
def test_raw_csv_keeps_raw_cookie_session_for_non_masking_caller(client, monkeypatch):
    df = pd.DataFrame({"cookie_session": ["a1b2c3d4deadbeef"], "url": ["/a"]})
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)

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
        kwargs = stub_aggregates.call_args.kwargs
        assert kwargs["include_top_n"] is exp_top_n
        for flag in ("include_time_series", "include_conn_requests", "include_map_data"):
            assert kwargs[flag] is exp_page_flags
    elif endpoint == "bundle":
        assert body.get("aggregates") is None
    if endpoint == "bundle":
        if exp_bots:
            assert body["top_bots"]["bots"] == [{"name": "Googlebot", "request_count": 42}]
        else:
            assert body.get("top_bots") is None


def test_bundle_gather_and_prewarm_structural_invariants():
    """Pin F015 return_exceptions=True and cancellation-guarded primary connection pre-warm."""
    src = inspect.getsource(dash.dashboard_bundle)
    assert "return_exceptions=True" in src
    assert "isinstance(aggregates, BaseException)" in src and "isinstance(top_bots, BaseException)" in src
    assert "await asyncio.to_thread(lambda: ctx.con)" not in src
    assert "ctx.con" in src and "asyncio.shield(" in src


# ── Parallel execution & Fallback scenarios ─────────────────────────────


@pytest.mark.parametrize("error_cls", [None, _PoolBusy, RuntimeError])
def test_bundle_parallel_checkout_and_fallbacks(client, stub_aggregates, stub_top_bots, error_cls):
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = MagicMock()
    patch_kw = {"side_effect": error_cls("err")} if error_cls else {"return_value": mock_cm}
    with patch("backend.core.duckdb_pool.checkout_connection", **patch_kw) as mock_checkout:
        resp = client.post("/api/dashboard/bundle", json=_BUNDLE_BODY)
        assert resp.status_code == 200
        mock_checkout.assert_called_once()
        if error_cls is None:
            mock_cm.__enter__.assert_called_once()
            mock_cm.__exit__.assert_called_once()


@pytest.mark.parametrize("error_cls", [None, _PoolBusy, RuntimeError])
def test_bundle_starts_aggregates_during_second_checkout(
    client, stub_aggregates, stub_top_bots, monkeypatch, error_cls
):
    aggregate_started = threading.Event()
    aggregate_finished = threading.Event()
    overlap = []
    second_con = object()
    cm = MagicMock()

    def enter():
        overlap.append(aggregate_started.wait(timeout=1))
        if error_cls is not None:
            raise error_cls("second checkout unavailable")
        return second_con

    def aggregates(**kwargs):
        aggregate_started.set()
        aggregate_finished.set()
        return stub_aggregates.return_value

    def bots(**kwargs):
        assert aggregate_finished.is_set() or kwargs["con"] is second_con
        assert (kwargs["con"] is second_con) is (error_cls is None)
        return stub_top_bots.return_value

    cm.__enter__.side_effect = enter
    stub_aggregates.side_effect = aggregates
    stub_top_bots.side_effect = bots
    monkeypatch.setattr("backend.core.duckdb_pool.checkout_connection", lambda *args, **kwargs: cm)

    response = client.post("/api/dashboard/bundle", json=_BUNDLE_BODY)

    assert response.status_code == 200
    assert overlap == [True]
    assert response.json()["aggregates"]["total_rows"] == 100
    assert response.json()["top_bots"]["bots"] == stub_top_bots.return_value["bots"]
    assert cm.__exit__.call_count == (1 if error_cls is None else 0)


async def test_bundle_cancellation_drains_checkout_and_both_workers(monkeypatch, stub_aggregates, stub_top_bots):
    checkout_started = threading.Event()
    release_checkout = threading.Event()
    aggregate_started = threading.Event()
    release_aggregate = threading.Event()
    bots_started = threading.Event()
    release_bots = threading.Event()
    cm = MagicMock()
    second_con = object()
    ctx = SimpleNamespace(
        source={"name": "bundle_cancel", "service_id": "bundle_cancel"},
        high_scale=None,
        con=object(),
        resolve_window=MagicMock(return_value=(_BUNDLE_BODY["start_time"], _BUNDLE_BODY["end_time"])),
        prewarm_con=AsyncMock(),
    )

    def enter():
        checkout_started.set()
        assert release_checkout.wait(timeout=5)
        return second_con

    def aggregates(**kwargs):
        aggregate_started.set()
        assert release_aggregate.wait(timeout=5)
        return stub_aggregates.return_value

    def bots(**kwargs):
        assert kwargs["con"] is second_con
        bots_started.set()
        assert release_bots.wait(timeout=5)
        return stub_top_bots.return_value

    cm.__enter__.side_effect = enter
    stub_aggregates.side_effect = aggregates
    stub_top_bots.side_effect = bots
    monkeypatch.setattr("backend.core.duckdb_pool.checkout_connection", lambda *args, **kwargs: cm)
    task = asyncio.create_task(dash.dashboard_bundle(AggregatesRequest(**_BUNDLE_BODY), ctx))
    try:
        assert await asyncio.to_thread(checkout_started.wait, 2)
        assert await asyncio.to_thread(aggregate_started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        cm.__exit__.assert_not_called()
        release_checkout.set()
        assert await asyncio.to_thread(bots_started.wait, 2)
        assert not task.done()
        cm.__exit__.assert_not_called()
    finally:
        release_checkout.set()
        release_bots.set()
        release_aggregate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    cm.__exit__.assert_called_once()


async def test_bundle_aggregate_error_waits_for_bots_before_returning_connection(
    monkeypatch, stub_aggregates, stub_top_bots
):
    bots_started = threading.Event()
    release_bots = threading.Event()
    aggregate_failed = threading.Event()
    cm = MagicMock()
    ctx = SimpleNamespace(
        source={"name": "bundle_error", "service_id": "bundle_error"},
        high_scale=None,
        con=object(),
        resolve_window=MagicMock(return_value=(_BUNDLE_BODY["start_time"], _BUNDLE_BODY["end_time"])),
        prewarm_con=AsyncMock(),
    )

    def aggregates(**kwargs):
        assert bots_started.wait(timeout=5)
        aggregate_failed.set()
        raise RuntimeError("aggregate failed")

    def bots(**kwargs):
        bots_started.set()
        assert release_bots.wait(timeout=5)
        return stub_top_bots.return_value

    stub_aggregates.side_effect = aggregates
    stub_top_bots.side_effect = bots
    monkeypatch.setattr("backend.core.duckdb_pool.checkout_connection", lambda *args, **kwargs: cm)
    task = asyncio.create_task(dash.dashboard_bundle(AggregatesRequest(**_BUNDLE_BODY), ctx))
    try:
        assert await asyncio.to_thread(aggregate_failed.wait, 2)
        await asyncio.sleep(0)
        assert not task.done()
        cm.__exit__.assert_not_called()
    finally:
        release_bots.set()
        with pytest.raises(HTTPException) as exc_info:
            await task
        assert exc_info.value.detail["error"] == "unhandled_error"
        assert exc_info.value.__cause__.args == ("aggregate failed",)
    cm.__exit__.assert_called_once()


@pytest.mark.parametrize("sections", [None, ["core"], ["bots"]])
async def test_cached_bundle_skips_connections_after_resolving_scoped_window(monkeypatch, sections):
    source = {"name": "cached_bundle", "service_id": "cached_bundle"}
    bounds = ("2026-06-12T00:15:00Z", "2026-06-12T00:45:00Z")
    ctx = SimpleNamespace(
        source=source,
        high_scale=None,
        resolve_window=MagicMock(return_value=bounds),
        prewarm_con=AsyncMock(side_effect=AssertionError("cached response checked out a connection")),
    )
    aggregates = {"total_rows": 7}
    bots = {"bots": [{"name": "Example bot", "request_count": 3}], "ngwaf_bots": []}
    aggregate_peek = MagicMock(return_value=aggregates)
    bot_peek = MagicMock(return_value=bots)
    monkeypatch.setattr(dash.repo, "peek_aggregates", aggregate_peek)
    monkeypatch.setattr(security_repo, "peek_top_bots", bot_peek, raising=False)
    checkout = MagicMock(side_effect=AssertionError("cached response acquired a second connection"))
    monkeypatch.setattr("backend.core.duckdb_pool.checkout_connection", checkout)
    req = AggregatesRequest(**_BUNDLE_BODY, fields=["country"], sections=sections)

    result = await dash.dashboard_bundle(req, ctx)

    ctx.resolve_window.assert_called_once_with(req)
    ctx.prewarm_con.assert_not_called()
    checkout.assert_not_called()
    want_aggregates = sections != ["bots"]
    want_bots = sections != ["core"]
    assert result["aggregates"] == (aggregates if want_aggregates else None)
    assert result["top_bots"] == (bots if want_bots else None)
    assert aggregate_peek.called is want_aggregates
    assert bot_peek.called is want_bots
    for peek in (aggregate_peek, bot_peek):
        if peek.called:
            assert peek.call_args.kwargs["src"] is source
            assert peek.call_args.kwargs["start_time"] == bounds[0]
            assert peek.call_args.kwargs["end_time"] == bounds[1]
            assert peek.call_args.kwargs["filters"] == req.filters
    if want_aggregates:
        assert aggregate_peek.call_args.kwargs["fields_filter"] == ["country"]
        assert aggregate_peek.call_args.kwargs["include_top_n"] is (sections is None)


@pytest.mark.parametrize("missing", ["aggregates", "bots"])
async def test_cached_bundle_partial_miss_keeps_existing_reader_path(monkeypatch, missing):
    source = {"name": "partial_bundle", "service_id": "partial_bundle"}
    ctx = SimpleNamespace(
        source=source,
        high_scale=None,
        con=object(),
        resolve_window=MagicMock(return_value=("2026-06-12T00:00:00Z", "2026-06-12T01:00:00Z")),
        prewarm_con=AsyncMock(),
    )
    aggregates = {"total_rows": 7}
    bots = {"bots": [], "ngwaf_bots": []}
    get_aggregates = MagicMock(return_value=aggregates)
    get_aggregates.__module__ = "backend.repositories.dashboard"
    get_bots = MagicMock(return_value=bots)
    get_bots.__module__ = "backend.repositories.security"
    monkeypatch.setattr(dash.repo, "get_aggregates", get_aggregates)
    monkeypatch.setattr(security_repo, "get_top_bots", get_bots)
    monkeypatch.setattr(
        dash.repo, "peek_aggregates", MagicMock(return_value=None if missing == "aggregates" else aggregates)
    )
    monkeypatch.setattr(
        security_repo, "peek_top_bots", MagicMock(return_value=None if missing == "bots" else bots), raising=False
    )
    monkeypatch.setattr(
        "backend.core.duckdb_pool.checkout_connection", MagicMock(side_effect=RuntimeError("no second connection"))
    )

    result = await dash.dashboard_bundle(AggregatesRequest(), ctx)

    ctx.prewarm_con.assert_awaited_once()
    get_aggregates.assert_called_once()
    get_bots.assert_called_once()
    assert get_aggregates.call_args.kwargs["con"] is ctx.con
    assert get_bots.call_args.kwargs["con"] is ctx.con
    assert result["aggregates"] == aggregates
    assert result["top_bots"] == bots


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


async def test_bundle_resolves_primary_connection_off_event_loop(monkeypatch):
    class _BlockingHolder:
        def __init__(self, con):
            self.con = None
            self._real = con
            self.enter_calls = 0

        def __enter__(self):
            self.enter_calls += 1
            _time.sleep(0.25)
            self.con = self._real
            return self._real

        def __exit__(self, *_a):
            return False

    holder = _BlockingHolder(MagicMock())
    ctx = RequestContext(
        service_id="svc-standard-not-highscale",
        source={"name": "svc", "service_id": "svc-standard-not-highscale"},
        telemetry=MagicMock(),
        _holder=holder,
    )
    monkeypatch.setattr(dash.repo, "get_aggregates", lambda **_k: {})
    req = AggregatesRequest(**_BUNDLE_BODY, sections=["core"])
    _, ticks = await _measure_heartbeat_ticks(dash.dashboard_bundle(req, ctx))
    assert holder.enter_calls >= 1 and ticks >= 10


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
        _holder=MagicMock(),
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
            [
                RuntimeError("Cannot open file '/srv/data/buffers/x.parquet'"),
                RuntimeError("IO Error: file '/srv/data/ducklake/snap.json' is corrupt"),
            ],
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
    with patch("backend.repositories.query.execute_query", side_effect=side_effect):
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
        ("Cannot open '/var/lib/duckdb/foo.db'", "/var/lib/duckdb/foo.db"),
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


def test_admin_and_analyst_query_window_and_mask_forwarding(client, in_memory_duckdb, test_service_source):
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_result()

    with patch("backend.repositories.query.execute_query", side_effect=_fake):
        resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT 1"})
    assert resp.status_code == 200 and captured["time_filter"] is None and captured["mask_ips"] is False

    start = datetime.datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime.datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-1", pii_policy={"mask_ips": True}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )
    captured.clear()
    with patch("backend.repositories.query.execute_query", side_effect=_fake):
        resp2 = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT ip FROM logs"})
    assert resp2.status_code == 200, resp2.text
    assert captured["time_filter"] == (start.isoformat(), end.isoformat()) and captured["mask_ips"] is True


# ── /api/presets: source lookup + connection fallback ───────────────────────


def test_presets_source_resolution_and_no_connection_acquired(client):
    with patch("backend.core.request_context.svcconfig.get_active_service_id", return_value=None):
        resp_no_svc = client.get("/api/presets")
    assert resp_no_svc.status_code == 200 and resp_no_svc.json() == []

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        resp_unknown = client.get("/api/presets", headers={"x-fastly-service-id": "ghost-service"})
    assert resp_unknown.status_code == 200 and resp_unknown.json() == []

    fake_presets = [{"id": "p1", "name": "Top 5xx", "description": "Top 5xx URLs", "sql": "SELECT 1"}]
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": MOCK_SERVICE_ID},
        ),
        patch("backend.core.duckdb.get_connection") as mock_get_connection,
        patch("backend.repositories.query.get_presets", return_value=fake_presets),
    ):
        resp_ok = client.get("/api/presets", headers=_Q_HEADERS)
    assert resp_ok.status_code == 200 and resp_ok.json() == fake_presets and mock_get_connection.call_count == 0


# ── End-to-end router + repository SELECT, AST safety, schema & masking ─────


def test_query_select_executes_against_duckdb_and_stale_view_retry(client, in_memory_duckdb, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    in_memory_duckdb.execute(
        "CREATE TABLE IF NOT EXISTS logs_test_service (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR, status INTEGER)"
    )
    in_memory_duckdb.execute(
        "INSERT INTO logs_test_service (timestamp, ip, url, status) "
        "VALUES (TIMESTAMPTZ '2026-06-17T10:00:00Z', '203.0.113.42', '/index.html', 200)"
    )
    resp = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT url, status FROM logs"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["columns"] == ["url", "status"] and body["row_count"] == 1
    assert body["data"][0] == {"url": "/index.html", "status": 200}
    assert isinstance(body["_debug_queries"], list) and len(body["_debug_queries"]) >= 1

    with patch("backend.core.storage.execute_with_stale_view_retry", return_value=_stub_result()) as mock_retry:
        resp_retry = client.post("/api/query", headers=_Q_HEADERS, json={"sql": "SELECT 1"})
    assert resp_retry.status_code == 200 and mock_retry.call_count == 1


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


def test_query_remote_analyst_masks_ips_end_to_end(client, in_memory_duckdb, test_service_source):
    in_memory_duckdb.execute(
        "CREATE TABLE IF NOT EXISTS logs_test_service (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR, status INTEGER)"
    )
    in_memory_duckdb.execute(
        "INSERT INTO logs_test_service (timestamp, ip, url, status) "
        "VALUES (TIMESTAMPTZ '2026-06-17T09:30:00Z', '198.51.100.77', '198.51.100.88', 200)"
    )
    start = datetime.datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime.datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-mask", pii_policy={"mask_ips": True}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )
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

    with (
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.duckdb.get_schema", return_value=[]),
    ):
        resp_fb = client.get(f"/api/query/schema/{MOCK_SERVICE_ID}")
    assert resp_fb.status_code == 200
    assert {"timestamp", "url", "status"}.issubset({c["column"] for c in resp_fb.json()["schema"]})


@pytest.mark.security_regression
def test_query_high_scale_is_closed_to_analysts_until_clickhouse_enforces_the_boundary(
    client, in_memory_duckdb, test_service_source
):
    calls: list[str] = []
    fake_hs_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)
    session = SimpleNamespace(
        session_id="sess-hs", pii_policy={"mask_ips": True}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
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


def _analyst_on_high_scale(client, in_memory_duckdb, test_service_source, main_calls, console, mask_ips=True):
    main_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: main_calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=main_client)
    session = SimpleNamespace(
        session_id="sess-hs2", pii_policy={"mask_ips": mask_ips}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
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
    client, in_memory_duckdb, test_service_source
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
    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, main_calls, SimpleNamespace(client_for=client_for)
    )
    assert resp.status_code == 200 and resp.json()["data"] == [{"n": 7}]
    sql, settings = seen[0]
    assert "fla_cv_view_req" in sql and "request_facts" not in sql
    scope = settings["additional_table_filters"]
    assert "event_timestamp >=" in scope and "service_id" not in scope
    assert main_calls == [] and asked == [True]


@pytest.mark.security_regression
def test_an_invite_without_mask_ips_gets_the_raw_identity(client, in_memory_duckdb, test_service_source):
    asked: list[bool] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in sql else [{"n": 1}]
    )

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, [], SimpleNamespace(client_for=client_for), mask_ips=False
    )
    assert resp.status_code == 200 and asked == [False]


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_no_console_identity_is_available(
    client, in_memory_duckdb, test_service_source
):
    main_calls: list[str] = []
    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, main_calls, SimpleNamespace(client_for=lambda sid: None)
    )
    assert resp.status_code == 403 and resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert main_calls == []


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_the_identity_fails_its_isolation_check(
    client, in_memory_duckdb, test_service_source
):
    def refuse(sid):
        raise RuntimeError("console identity failed the row isolation check")

    main_calls: list[str] = []
    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, main_calls, SimpleNamespace(client_for=refuse)
    )
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
    client, in_memory_duckdb, test_service_source, policy, expected_masked
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
        _con_override=in_memory_duckdb,
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

__all__ = ["setup_temp_rum_db"]

SVC = "svc-rum-admin-test"
GOLDEN_DIR = Path(__file__).parent / "golden"
_GOLDEN_SERVICE_ID = "svc-rum-golden"
_TOKEN_SERVICE_ID = "svc-rum-token"
WINDOW = "start_time=2026-08-05T00:00:00Z&end_time=2026-08-05T23:59:59Z"
ANCHOR = "2026-08-05T12:00:30Z"
EMPTY = {"no_data": True, "vitals_rows": []}

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


def _mock_rum_source(db_path: Any) -> dict[str, str]:
    return {
        "name": "test_service",
        "service_id": "test_service",
        "duckdb_path": str(db_path).replace(".rum.duckdb", ".duckdb"),
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }


@pytest.fixture
def skip_view_update(request):
    if request.node.name in ("test_rum_analytics_cold_start_readonly", "test_rum_live_buffer_unlink_race_self_heals"):
        yield None
    else:
        with patch("backend.core.storage._core.update_storage_view") as mock:
            yield mock


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
    ("cfg", "cur_sha", "leg_sha", "expected_drift", "expected_deployed", "expect_save"),
    [
        (_status_cfg("sha-current", _FULL_RUM_CFG), "sha-current", "sha-legacy", False, "sha-current", False),
        (
            _status_cfg("sha-old-deployed", {}),
            "sha-new-template",
            "sha-legacy-unrelated",
            True,
            "sha-old-deployed",
            False,
        ),
        (_status_cfg("sha-legacy-match", {}), "sha-new-v2", "sha-legacy-match", False, "sha-new-v2", True),
        (_status_cfg(None), "freshsha123", "sha-legacy", False, "freshsha123", False),
    ],
)
def test_admin_rum_status_variants(cfg, cur_sha, leg_sha, expected_drift, expected_deployed, expect_save):
    test_client = TestClient(_make_app(analyst=False))
    mock_save = MagicMock()
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save)),
        patch("backend.routers.rum.rum_vcl_fingerprint", return_value=cur_sha),
        patch("backend.routers.rum.legacy_rum_vcl_fingerprint", return_value=leg_sha),
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


def _b(
    pathname: str,
    load_time: float = 1.5,
    lcp: float | None = None,
    cls: float | None = None,
    inp: float | None = None,
    **kw,
) -> dict[str, Any]:
    out: dict[str, Any] = {"pathname": pathname, "load_time": load_time, **kw}
    for k, v in (("lcp", lcp), ("cls", cls), ("inp", inp)):
        if v is not None:
            out[k] = v
    return out


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_fallback_and_live_events(setup_temp_rum_db) -> None:
    response = client.get("/api/services/svc-test-rum-fallback/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["no_data"] is True and data["beacon_count"] == 0
    assert data["worst_pages"] == [] and data["errors"] == [] and data["trends"]["lcp"] == []
    assert client.get("/api/services/svc-test-rum-fallback/rum/live-events").json() == []

    _insert_beacons(
        setup_temp_rum_db,
        [_b("/live-test", 1.5, 1.7, 0.01, 100, meta={"browser": "Chrome", "os": "macOS", "device": "Desktop"})],
    )
    events = client.get("/api/services/svc-test-rum-fallback/rum/live-events").json()
    assert len(events) == 4 and events[0]["path"] == "/live-test"
    assert events[0]["browser"] == "Chrome" and events[0]["os"] == "macOS"


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_real_data(setup_temp_rum_db) -> None:
    service_id = "test_service_real_data"
    err_null = [{"value": "TypeError: null is not an object", "filename": "main.js"}]
    err_ref = [{"value": "ReferenceError: x is not defined", "filename": "checkout.js"}]
    rows = [
        ("/pricing", 2.1, 2.3, 0.02, 120, "Chrome", "macOS", "Desktop", err_null, ""),
        ("/", 1.4, 1.6, 0.01, 80, "Chrome", "macOS", "Desktop", None, None),
        ("/pricing", 2.5, 2.8, 0.03, 150, "Safari", "iOS", "Mobile", None, None),
        ("/docs", 1.8, 1.9, 0.05, 90, "Firefox", "Linux", "Desktop", None, None),
        ("/", 1.2, 1.4, 0.01, 70, "Chrome", "Windows", "Desktop", None, None),
        ("/checkout", 3.2, 3.5, 0.12, 220, "Chrome", "macOS", "Desktop", err_null, None),
        ("/pricing", 2.0, 2.2, 0.02, 110, "Chrome", "macOS", "Desktop", None, None),
        ("/", 1.5, 1.7, 0.02, 85, "Safari", "macOS", "Desktop", None, None),
        ("/docs", 1.9, 2.1, 0.04, 100, "Chrome", "Android", "Mobile", None, None),
        ("/", 1.3, 1.5, 0.01, 75, "Edge", "Windows", "Desktop", None, None),
        ("/pricing", 2.2, 2.4, 0.02, 130, "Chrome", "macOS", "Desktop", None, None),
        ("/checkout", 3.0, 3.2, 0.10, 200, "Chrome", "macOS", "Desktop", err_ref, None),
    ]
    test_beacons = [
        _b(
            p,
            lt,
            lcp,
            cls,
            inp,
            meta={"browser": b, "os": o, "device": d},
            **({"exceptions": exc} if exc is not None else {}),
            **({"req_id": rid} if rid is not None else {}),
        )
        for p, lt, lcp, cls, inp, b, o, d, exc, rid in rows
    ]
    _insert_beacons(setup_temp_rum_db, test_beacons)

    response = client.get(f"/api/services/{service_id}/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False
    assert data["vitals"]["lcp"]["distribution"]["good"] > 0 and data["vitals"]["lcp"]["distribution"]["poor"] == 0
    assert data["environments"]["browsers"].get("Chrome", 0) == 8
    assert data["environments"]["os"].get("macOS", 0) == 7
    assert data["environments"]["devices"].get("Desktop", 0) == 10
    assert [e["count"] for e in data["errors"]] == [2, 1]
    assert len(client.get(f"/api/services/{service_id}/rum/live-events").json()) >= 12


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize("window_hours", [0.5, 24])
def test_rum_analytics_serves_without_in_request_recompute(setup_temp_rum_db, window_hours: float) -> None:
    import backend.core.duckdb as _duckdb_mod

    service_id = f"test_service_no_recompute_{window_hours}"
    now = datetime.datetime.now(datetime.UTC)
    offset_mins = 2 if window_hours < 1 else 60
    _insert_beacons(
        setup_temp_rum_db,
        [
            _b(
                f"/p_{i}",
                1.4,
                1.8,
                0.02,
                received_at=(now - datetime.timedelta(minutes=offset_mins + i)).isoformat(),
                req_id=f"rid_{i}",
            )
            for i in range(12)
        ],
    )
    start, end = iso_z(now - datetime.timedelta(hours=window_hours)), iso_z(now)
    real_get_connection = _duckdb_mod.get_connection
    write_conn_calls = []

    def _spy_get_connection(*args, **kwargs):
        if kwargs.get("read_only") is False:
            write_conn_calls.append((args, kwargs))
        return real_get_connection(*args, **kwargs)

    with (
        patch("backend.core.rollups.recompute.recompute_rum_aggregates") as mock_recompute,
        patch.object(_duckdb_mod, "get_connection", side_effect=_spy_get_connection),
    ):
        response = client.get(f"/api/services/{service_id}/rum/analytics?start_time={start}&end_time={end}")

    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["beacon_count"] > 0
    mock_recompute.assert_not_called()
    assert write_conn_calls == []


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_date_filtering_and_shared_distinct_id(setup_temp_rum_db) -> None:
    service_id = "test_service_date_filter"
    _insert_beacons(
        setup_temp_rum_db,
        [
            _b(f"/page_{i}", 1.5, received_at="2026-08-05T10:00:00+00:00" if i < 10 else "2026-08-05T12:00:00+00:00")
            for i in range(20)
        ],
    )
    data_after = client.get(f"/api/services/{service_id}/rum/analytics?start_time=2026-08-05T11:00:00+00:00").json()
    assert all(int(p["path"].split("_")[1]) >= 10 for p in data_after["worst_pages"])
    data_before = client.get(f"/api/services/{service_id}/rum/analytics?end_time=2026-08-05T11:00:00+00:00").json()
    assert all(int(p["path"].split("_")[1]) < 10 for p in data_before["worst_pages"])

    con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
    try:
        con.execute("DELETE FROM client_vitals; DELETE FROM client_errors;")
        shared_ts = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
        other_ts = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=6)
        env = ("/", "Chrome", "macOS", "Desktop")
        geo = ("", "Austin", "TX", "US", "SIN", "1.3", 45.0)
        con.executemany(
            "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (shared_ts, "lcp", 2.1, "good", *env, "shared", *geo),
                (shared_ts, "event_click", 1.0, "", *env, "shared", *geo),
            ],
        )
        con.execute(
            "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (other_ts, "TypeError", "main.js", 0, 0, *env, "other", *geo),
        )
    finally:
        con.close()

    dedup = client.get("/api/services/test_service_shared_distinct_id/rum/analytics").json()
    assert dedup["beacon_count"] == 2 and dedup["pageview_count"] == 1 and dedup["error_count"] == 1


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize(
    ("mode", "browser_val", "expected_browsers", "expected_count"),
    [
        ("include", "Chrome", {"Chrome": 8}, 8),
        ("include", "Safari", {"Safari": 7}, 7),
        ("include", "Firefox", {}, 0),
        ("exclude", "Chrome", {"Safari": 7}, 7),
    ],
)
def test_rum_analytics_with_filters(
    setup_temp_rum_db, mode: str, browser_val: str, expected_browsers: dict, expected_count: int
) -> None:
    service_id = "test_service_with_filters"
    _insert_beacons(
        setup_temp_rum_db,
        [
            _b(
                f"/page_{i}",
                2.0,
                2.1,
                0.02,
                100,
                meta={
                    "browser": "Chrome" if i < 8 else "Safari",
                    "os": "macOS" if i < 10 else "iOS",
                    "device": "Desktop" if i < 12 else "Mobile",
                },
            )
            for i in range(15)
        ],
    )
    filters = json.dumps({"browser": {"mode": mode, "values": [browser_val]}})
    response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["beacon_count"] == expected_count
    if expected_browsers:
        assert data["environments"]["browsers"] == expected_browsers


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_analyst_request_outside_invite_window_is_clamped(setup_temp_rum_db) -> None:
    service_id = "test_rum_analytics_clamp_analyst"
    old_ts = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=30)).isoformat()
    recent_ts = datetime.datetime.now(datetime.UTC).isoformat()
    _insert_beacons(
        setup_temp_rum_db,
        [
            {"pathname": "/thirty-days-old", "load_time": 1.5, "received_at": old_ts},
            {"pathname": "/just-now", "load_time": 1.5, "received_at": recent_ts},
        ],
    )
    src = _mock_rum_source(setup_temp_rum_db)
    analyst_sess = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    analyst_req = _fake_request(analyst_sess)
    analyst_ctx = make_test_request_context(
        service_id=service_id,
        source=src,
        session=analyst_sess,
        read_only=True,
        time_bounds=get_analyst_time_bounds(typing.cast(typing.Any, analyst_req)),
    )
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


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_cold_start_readonly(tmp_path):
    from backend.core.storage._core import update_storage_view

    temp_db_path = tmp_path / "cold_start.rum.duckdb"
    duckdb.connect(str(temp_db_path)).close()
    ro_con = duckdb.connect(str(temp_db_path), read_only=True)
    try:
        source = {
            "name": "cold_start::rum",
            "service_id": "cold_start",
            "duckdb_path": str(tmp_path / "cold_start.duckdb"),
        }
        update_storage_view(ro_con, source)
        assert ro_con.execute("SELECT COUNT(*) FROM client_vitals").fetchone()[0] == 0
        assert ro_con.execute("SELECT COUNT(*) FROM client_errors").fetchone()[0] == 0
    finally:
        ro_con.close()


@pytest.mark.usefixtures("skip_view_update")
def test_rum_live_buffer_unlink_race_self_heals(tmp_path, monkeypatch) -> None:
    from backend.core import duckdb as db_mod
    from backend.core.storage._core import clear_source_caches, update_storage_view
    from backend.repositories import rum as rum_repo

    monkeypatch.setattr(db_mod, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("bucket", "default")))
    vitals_ddl = "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE"
    errors_ddl = "timestamp TIMESTAMPTZ, error_message VARCHAR, error_file VARCHAR, error_line INTEGER, error_col INTEGER, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE"
    buf_vitals_dir = tmp_path / "cache" / "bucket-race" / "buffer" / "client_vitals"
    buf_errors_dir = tmp_path / "cache" / "bucket-race" / "buffer" / "client_errors"
    buf_vitals_dir.mkdir(parents=True, exist_ok=True)
    buf_errors_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.UTC)
    ts1, ts2 = (now - datetime.timedelta(seconds=20)).isoformat(), (now - datetime.timedelta(seconds=10)).isoformat()
    batch1_v, batch2_v = buf_vitals_dir / "batch_001.parquet", buf_vitals_dir / "batch_002.parquet"
    batch1_e, batch2_e = buf_errors_dir / "batch_001.parquet", buf_errors_dir / "batch_002.parquet"

    seed_con = duckdb.connect(":memory:")
    try:
        seed_con.execute(f"CREATE TABLE cv ({vitals_ddl}); CREATE TABLE ce ({errors_ddl});")
        for target, val, path, cid, req, ts in [
            (batch1_v, 1.8, "/surviving", "cid1", "req1", ts1),
            (batch2_v, 2.1, "/unlinked", "cid2", "req2", ts2),
        ]:
            seed_con.execute("DELETE FROM cv")
            seed_con.execute(
                "INSERT INTO cv VALUES (?, 'lcp', ?, 'good', ?, 'Chrome', 'macOS', 'Desktop', ?, ?, 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 30.0)",
                [ts, val, path, cid, req],
            )
            seed_con.execute(f"COPY cv TO '{target}' (FORMAT PARQUET)")
        for target, msg, line, col, path, cid, req, ts in [
            (batch1_e, "ReferenceError: x", 10, 2, "/surviving", "cid3", "req3", ts1),
            (batch2_e, "TypeError: y", 20, 4, "/unlinked", "cid4", "req4", ts2),
        ]:
            seed_con.execute("DELETE FROM ce")
            seed_con.execute(
                "INSERT INTO ce VALUES (?, ?, 'app.js', ?, ?, ?, 'Chrome', 'macOS', 'Desktop', ?, ?, 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 30.0)",
                [ts, msg, line, col, path, cid, req],
            )
            seed_con.execute(f"COPY ce TO '{target}' (FORMAT PARQUET)")
    finally:
        seed_con.close()

    rum_db_path = tmp_path / "svc_race.rum.duckdb"
    source = {
        "name": "svc_race::rum",
        "service_id": "svc_race",
        "bucket": "bucket-race",
        "duckdb_path": str(tmp_path / "svc_race.duckdb"),
        "local_buffer_dir": str(tmp_path / "cache" / "bucket-race" / "buffer"),
    }

    con = duckdb.connect(str(rum_db_path), read_only=False)
    try:
        clear_source_caches("svc_race")
        update_storage_view(con, source, force=True)
        batch2_v.unlink()

        start_iso, end_iso = (now - datetime.timedelta(minutes=15)).isoformat(), now.isoformat()
        events = rum_repo.fetch_rum_live_events(
            con, where_sql="timestamp >= ? AND timestamp <= ?", params=[start_iso, end_iso], source=source
        )
        assert "/surviving" in {e[2] for e in events}

        clear_source_caches("svc_race")
        update_storage_view(con, source, force=True)
        batch2_e.unlink()

        analytics = rum_repo.fetch_rum_analytics(
            con,
            service_id="svc_race",
            start_time=start_iso,
            end_time=end_iso,
            parsed_filters={},
            where_sql="timestamp >= ? AND timestamp <= ?",
            params=[start_iso, end_iso],
            raw_window_max_s=7200,
            source=source,
        )
        assert analytics["no_data"] is False and analytics["total_beacons"] == 2 and analytics["errors_count"] == 1

        seed_con2 = duckdb.connect(":memory:")
        try:
            seed_con2.execute(f"CREATE TABLE cv ({vitals_ddl}); COPY cv TO '{batch2_v}' (FORMAT PARQUET);")
        finally:
            seed_con2.close()
        clear_source_caches("svc_race")
        update_storage_view(con, source, force=True)
        batch2_v.unlink()

        recent_count, last_dt = rum_repo.get_beacon_health(con)
        assert recent_count >= 1 and last_dt is not None
    finally:
        con.close()


@pytest.mark.usefixtures("skip_view_update")
def test_rum_long_window_merges_live_active_hour_tail_without_double_counting(setup_temp_rum_db) -> None:
    from backend.core.rollups.recompute import recompute_rum_aggregates

    service_id = "test_service_long_window_live_tail"
    now = datetime.datetime.now(datetime.UTC).replace(minute=30, second=0, microsecond=0)
    three_hours_ago, ten_mins_ago, five_secs_ago = (
        now - datetime.timedelta(hours=3),
        now - datetime.timedelta(minutes=10),
        now - datetime.timedelta(seconds=5),
    )

    initial_beacons = [
        _b(
            "/historical",
            1.2,
            1.5,
            0.01,
            received_at=(three_hours_ago + datetime.timedelta(minutes=i)).isoformat(),
            req_id=f"hist_{i}",
            cid=f"cid_hist_{i}",
        )
        for i in range(10)
    ] + [
        _b(
            "/active-early",
            1.4,
            1.8,
            0.02,
            received_at=(ten_mins_ago + datetime.timedelta(minutes=i)).isoformat(),
            req_id=f"early_{i}",
            cid=f"cid_early_{i}",
        )
        for i in range(2)
    ]
    _insert_beacons(setup_temp_rum_db, initial_beacons)

    con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
    try:
        recompute_rum_aggregates(con, service_id)
    finally:
        con.close()

    live_beacon = _b(
        "/live-5s-ago",
        2.5,
        2.9,
        0.04,
        received_at=five_secs_ago.isoformat(),
        req_id="live_5s_req",
        cid="cid_live_5s",
        exceptions=[{"value": "Error: live_5s_boom", "filename": "live.js", "lineno": 42, "colno": 7}],
    )
    _insert_beacons(setup_temp_rum_db, [live_beacon])

    start_1h, end_iso = iso_z(now - datetime.timedelta(hours=1)), iso_z(now)
    data_1h = client.get(f"/api/services/{service_id}/rum/analytics?start_time={start_1h}&end_time={end_iso}").json()
    assert data_1h["beacon_count"] == 3 and data_1h["error_count"] == 1

    _insert_beacons(setup_temp_rum_db, [live_beacon])
    for hours in (24, 24 * 30):
        start_long = iso_z(now - datetime.timedelta(hours=hours))
        data_long = client.get(
            f"/api/services/{service_id}/rum/analytics?start_time={start_long}&end_time={end_iso}"
        ).json()
        assert data_long["beacon_count"] == 13 and data_long["error_count"] == 1
        assert any(p["path"] == "/live-5s-ago" for p in data_long["worst_pages"])


def _faro_beacon(path: str, lcp: float, cls: float, inp: float, load_time: float) -> dict:
    return {
        "meta": {
            "page": {"url": f"https://example.test{path}"},
            "browser": {"name": "Chrome", "version": "120.0", "mobile": False},
            "os": {"name": "macOS", "version": "14.0"},
        },
        "events": [{"name": "faro.performance.navigation", "attributes": {"pageLoadTime": str(load_time)}}],
        "measurements": [{"type": "web-vitals", "values": {"lcp": lcp, "cls": cls, "inp": inp}}],
    }


@pytest.mark.usefixtures("skip_view_update")
@time_machine.travel("2026-09-22 12:30:00+00:00", tick=False)
def test_faro_shaped_beacons_vitals_exceptions_url_fallback_and_500_masking(setup_temp_rum_db, monkeypatch):
    _insert_beacons(
        setup_temp_rum_db, [_faro_beacon("/home", lcp, lcp / 2, lcp * 50, lcp) for lcp in (1.0, 2.0, 3.0, 4.0)]
    )
    data = client.get("/api/services/test_vitals_faro_shape/rum/analytics").json()
    assert data["vitals"]["lcp"]["p75"] == 3.25 and data["vitals"]["cls"]["p75"] == 1.625
    assert data["vitals"]["inp"]["p75"] == 162 and data["worst_pages"][0]["avg_load_time"] == 2.5

    con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
    try:
        con.execute("DELETE FROM client_vitals; DELETE FROM client_errors;")
    finally:
        con.close()

    exc_beacon = {
        **_faro_beacon("/checkout", 2.0, 0.02, 100, 2.0),
        "exceptions": [
            {
                "value": "TypeError: boom",
                "type": "TypeError",
                "stacktrace": {"frames": [{"filename": "app.js", "lineno": 42, "colno": 7}]},
            }
        ],
    }
    url_fallback_beacon = {
        "meta": {
            "page": {"url": "https://example.test/from-url//nested"},
            "browser": {"name": "Chrome"},
            "os": {"name": "macOS"},
        },
        "events": [{"name": "faro.performance.navigation", "attributes": {"pageLoadTime": "1.5"}}],
        "measurements": [{"type": "web-vitals", "values": {"lcp": 1.5, "cls": 0.02, "inp": 80}}],
    }
    _insert_beacons(setup_temp_rum_db, [exc_beacon] + [url_fallback_beacon for _ in range(10)])
    data2 = client.get("/api/services/test_vitals_faro_exception/rum/analytics").json()
    assert data2["errors"][0]["message"] == "TypeError: boom"
    assert data2["errors"][0]["file"] == "app.js" and data2["errors"][0]["line"] == 42
    assert "/from-url//nested" in [p["path"] for p in data2["worst_pages"]]

    monkeypatch.setattr(
        rum_router,
        "execute_with_stale_view_retry",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("simulated DB corruption mid-query")),
    )
    r_err = client.get("/api/services/test_vitals_mock_fallback/rum/analytics")
    assert r_err.status_code == 500 and r_err.json()["detail"]["message"] == "RUM analytics query failed"
    assert "simulated DB corruption" not in r_err.text


# ── Section 4: RUM JS Assets & Beacon Health ─────────────────────────────────


@pytest.mark.usefixtures("skip_view_update")
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


@pytest.mark.usefixtures("skip_view_update")
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


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize(
    ("mode", "expected_recent", "expected_complete", "expected_msg_substr"),
    [
        ("recent", 1, True, "Script installed and firing"),
        ("stale", 0, True, "Script installed and firing"),
        ("empty", 0, False, "Waiting for beacons..."),
        ("db_failure", 0, False, "db is locked"),
    ],
)
def test_beacon_health_states(
    setup_temp_rum_db, monkeypatch, mode: str, expected_recent: int, expected_complete: bool, expected_msg_substr: str
):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    monkeypatch.setattr(
        "backend.config.load_config", lambda sid: {"service_id": sid, "rum": {"enabled": True}, "rum_enabled": True}
    )
    now = datetime.datetime.now(datetime.UTC)
    if mode in ("recent", "stale"):
        delta = datetime.timedelta(minutes=5) if mode == "recent" else datetime.timedelta(hours=3)
        _insert_beacons(
            setup_temp_rum_db,
            [_b("/", 1.5, received_at=(now - delta).isoformat(), cid="cid1", req_id="req1")],
        )
    elif mode == "db_failure":
        monkeypatch.setattr(
            "backend.routers.rum.execute_with_stale_view_retry",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db is locked")),
        )

    r = client.get(f"/api/services/test_beacon_health_{mode}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True and data["recent_beacons"] == expected_recent
    assert data["setup_complete"] is expected_complete and expected_msg_substr in data["message"]


# ── Section 5: Golden Contracts & Token Window Resolution ────────────────────


def _beacons() -> list[dict]:
    specs = [
        ("10:05:00", "/home", "Chrome", "macOS", "Desktop", 1.2, 2100.0, 0.05, 150.0, False),
        ("10:20:00", "/home", "Firefox", "Windows", "Desktop", 2.4, 3400.0, 0.15, 320.0, False),
        ("10:40:00", "/cart", "Chrome", "Android", "Mobile", 3.1, 4800.0, 0.3, 650.0, True),
        ("11:10:00", "/cart", "Safari", "macOS", "Desktop", 0.9, 1800.0, 0.02, 90.0, True),
        ("11:50:00", "/search", "Chrome", "Windows", "Desktop", 1.7, 2600.0, 0.11, 210.0, False),
        ("13:30:00", "/home", "Chrome", "macOS", "Desktop", 1.1, 2000.0, 0.04, 120.0, False),
    ]
    return [
        _b(
            path,
            load,
            lcp,
            cls,
            inp,
            received_at=f"2026-08-05T{hms}+00:00",
            meta={"browser": browser, "os": os_, "device": device},
            cid=f"cid_{i}",
            req_id=f"req_{i}",
            **({"exceptions": [{"value": f"Boom {i}", "filename": "app.js"}] * (2 if i == 2 else 1)} if err else {}),
        )
        for i, (hms, path, browser, os_, device, load, lcp, cls, inp, err) in enumerate(specs)
    ]


def _check_golden(name: str, body: Any) -> None:
    if isinstance(body, dict):
        body = {k: v for k, v in body.items() if not k.startswith("_debug")}
    path = GOLDEN_DIR / f"rum_{name}.json"
    rendered = json.dumps(body, indent=2, sort_keys=True) + "\n"
    if os.getenv("UPDATE_GOLDEN") == "1":
        path.write_text(rendered)
    assert path.exists(), f"missing golden {path.name}; run with UPDATE_GOLDEN=1"
    assert json.loads(path.read_text()) == json.loads(rendered)


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize(
    "case",
    [
        "analytics_empty",
        "analytics_raw_long",
        "analytics_raw_short",
        "analytics_filtered",
        "analytics_rollup",
        "live_events",
        "beacon_health_disabled",
    ],
)
def test_rum_golden_contracts(setup_temp_rum_db, case: str) -> None:
    import backend.core.duckdb_pool as duckdb_pool
    from backend.core.rollups.recompute import recompute_rum_aggregates

    base = f"/api/services/{_GOLDEN_SERVICE_ID}/rum"
    if case == "analytics_empty":
        _check_golden(case, client.get(f"{base}/analytics?{WINDOW}").json())
        return
    if case == "beacon_health_disabled":
        _check_golden(case, client.get(f"{base}/beacon-health").json())
        return
    if case == "live_events":
        beacons = []
        for i, b in enumerate(_beacons()):
            b_meta = {k: b[k] for k in ("pathname", "meta", "cid", "req_id")}
            beacons.append({**b_meta, "received_at": b["received_at"], "lcp": b["lcp"]})
            if i % 2:
                beacons.append(
                    {
                        **b_meta,
                        "received_at": b["received_at"].replace(":00+", ":30+"),
                        "exceptions": [{"value": f"Err {i}"}],
                    }
                )
        _insert_beacons(setup_temp_rum_db, beacons)
        _check_golden(case, {"events": client.get(f"{base}/live-events?{WINDOW}").json()})
        return

    _insert_beacons(setup_temp_rum_db, _beacons())
    if case == "analytics_raw_long":
        _check_golden(case, client.get(f"{base}/analytics?{WINDOW}").json())
    elif case == "analytics_raw_short":
        _check_golden(
            case, client.get(f"{base}/analytics?start_time=2026-08-05T10:00:00Z&end_time=2026-08-05T11:30:00Z").json()
        )
    elif case == "analytics_filtered":
        flt = json.dumps({"browser": {"mode": "include", "values": ["Chrome"]}})
        _check_golden(
            case,
            client.get(
                f"{base}/analytics",
                params={"start_time": "2026-08-05T00:00:00Z", "end_time": "2026-08-05T23:59:59Z", "filters": flt},
            ).json(),
        )
    elif case == "analytics_rollup":
        duckdb_pool.shutdown_all()
        con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
        try:
            recompute_rum_aggregates(con, _GOLDEN_SERVICE_ID)
            assert con.execute("SELECT COUNT(*) FROM rum_vitals_aggregates").fetchone()[0] > 0
        finally:
            con.close()
        _check_golden(case, client.get(f"{base}/analytics?{WINDOW}").json())


@pytest.fixture
def captured(setup_temp_rum_db):
    calls: list[dict] = []

    def fake(con, **kw):
        calls.append(kw)
        return EMPTY

    with patch("backend.routers.rum.rum_repo.fetch_rum_analytics", fake):
        yield calls


_WIN_FULL = ("2026-08-05T00:00:00Z", "2026-08-05T23:59:59Z")
_WIN_1H = ("2026-08-05T00:00:00Z", "2026-08-05T01:00:00Z")


@pytest.mark.usefixtures("skip_view_update")
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


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_analyst_token_window_is_clamped_to_invite(setup_temp_rum_db, captured) -> None:
    session = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    req = _fake_request(session)
    bounds = get_analyst_time_bounds(typing.cast(typing.Any, req))
    source = _mock_rum_source(setup_temp_rum_db)

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
    import backend.core.metadata.usage_log as usage_log_db
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

    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "demo_service")
    usage_log.log_telemetry_query("demo_service", "page-456", "req-789", "duckdb", "SELECT 42", 12.3)

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
