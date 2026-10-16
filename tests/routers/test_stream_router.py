"""Tests for the Live Log Streaming router and SSE event generator."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from backend.models.stream import StreamLogEvent
from backend.routers import stream as stream_mod
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS


class _FakeRequest:
    """Fake request with controllable disconnect state."""

    def __init__(self, disconnect_after_ticks: int = 1, analyst_session: Any = None) -> None:
        self._ticks = 0
        self._disconnect_after = disconnect_after_ticks
        self.state = MagicMock()
        self.state.analyst_session = analyst_session

    async def is_disconnected(self) -> bool:
        self._ticks += 1
        return self._ticks > self._disconnect_after


class _MockHighScaleRegistry:
    def __init__(self, service: Any = None) -> None:
        self._service = service

    def resolve(self, service_id: str) -> Any:
        return self._service


# ── _format_stream_event tests ───────────────────────────────────────────────


def test_format_stream_event_basic():
    raw_row = {
        "timestamp": "2026-10-16T12:00:00Z",
        "client_ip": "198.51.100.42",
        "method": "POST",
        "url": "/api/v1/checkout",
        "status": 201,
        "pop": "SJC",
        "time_elapsed": 0.045,
        "fastly_service_id": "svc_test_123",
    }
    formatted = stream_mod._format_stream_event(raw_row, mask_ips=False)

    assert formatted["timestamp"] == "2026-10-16T12:00:00Z"
    assert formatted["client_ip"] == "198.51.100.42"
    assert formatted["method"] == "POST"
    assert formatted["url"] == "/api/v1/checkout"
    assert formatted["status"] == 201
    assert formatted["pop"] == "SJC"
    assert formatted["time_elapsed"] == 0.045
    assert formatted["attributes"]["fastly_service_id"] == "svc_test_123"
    assert len(formatted["event_id"]) > 0

    # Validates against Pydantic schema
    validated = StreamLogEvent(**formatted)
    assert validated.status == 201


def test_format_stream_event_ip_masking():
    raw_row = {
        "timestamp": "2026-10-16T12:00:00Z",
        "client_ip": "198.51.100.42",
        "remote_addr": "198.51.100.42",
        "server_ip": "203.0.113.1",
        "method": "GET",
        "url": "/",
        "status": 200,
    }
    formatted = stream_mod._format_stream_event(raw_row, mask_ips=True)

    assert formatted["client_ip"] != "198.51.100.42"
    assert "198.51.100" in formatted["client_ip"] or "xxx" in formatted["client_ip"] or "0" in formatted["client_ip"]
    assert formatted["attributes"]["client_ip"] == formatted["client_ip"]
    assert formatted["attributes"]["remote_addr"] == formatted["client_ip"]


def test_format_stream_event_fallback_fields():
    raw_row = {
        "time_to_first_byte": "0.123",
        "req_url": "/fallback",
        "request_method": "PATCH",
        "fastly_pop": "LCY",
        "ip": "1.2.3.4",
        "request_id": "custom-id-999",
    }
    formatted = stream_mod._format_stream_event(raw_row, mask_ips=False)

    assert formatted["event_id"] == "custom-id-999"
    assert formatted["url"] == "/fallback"
    assert formatted["method"] == "PATCH"
    assert formatted["pop"] == "LCY"
    assert formatted["client_ip"] == "1.2.3.4"
    assert formatted["time_elapsed"] == 0.123
    assert formatted["status"] == 200


# ── live_log_stream handler tests ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_live_log_stream_standard_mode_yields_events(monkeypatch):
    mock_source = {"service_id": "test_service", "backend": "duckdb"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: mock_source)

    sample_rows = [
        {"timestamp": "2026-10-16T12:00:01Z", "client_ip": "1.1.1.1", "url": "/page1", "status": 200},
        {"timestamp": "2026-10-16T12:00:02Z", "client_ip": "2.2.2.2", "url": "/page2", "status": 404},
    ]

    fetched_filters = {}

    def mock_fetch(source, limit=25, search=None, filters=None, mask_ips=False):
        fetched_filters["search"] = search
        fetched_filters["filters"] = filters
        fetched_filters["mask_ips"] = mask_ips
        return sample_rows

    monkeypatch.setattr(stream_mod, "_fetch_standard_stream_logs", mock_fetch)

    # Disconnect after 1 loop tick
    fake_req = _FakeRequest(disconnect_after_ticks=1)

    async def fast_sleep(_delay):
        pass

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    resp = await stream_mod.live_log_stream(
        request=fake_req,  # type: ignore[arg-type]
        service_id="test_service",
        filter="checkout",
        status="200",
        pop="SJC",
        client_ip="1.1.1.1",
        _access="read_write",
        registry=_MockHighScaleRegistry(None),  # type: ignore[arg-type]
    )

    assert resp.status_code == 200
    for header, value in SSE_PASSTHROUGH_HEADERS.items():
        assert resp.headers.get(header.lower()) == value

    agen = resp.body_iterator
    events = []
    async for frame in agen:
        events.append(frame)

    assert len(events) >= 2
    parsed = [json.loads(ev["data"]) for ev in events if isinstance(ev, dict) and "data" in ev]
    assert len(parsed) == 2
    assert parsed[0]["url"] == "/page2"  # reversed from newest to oldest
    assert parsed[1]["url"] == "/page1"
    assert fetched_filters["search"] == "checkout"
    assert fetched_filters["filters"]["status"]["values"] == ["200"]


@pytest.mark.asyncio
async def test_live_log_stream_deduplicates_events(monkeypatch):
    mock_source = {"service_id": "test_service"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: mock_source)

    row = {"event_id": "fixed-id-1", "timestamp": "2026-10-16T12:00:00Z", "url": "/test"}
    call_count = 0

    def mock_fetch(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return [row]

    monkeypatch.setattr(stream_mod, "_fetch_standard_stream_logs", mock_fetch)

    # 3 ticks before disconnecting
    fake_req = _FakeRequest(disconnect_after_ticks=3)

    async def fast_sleep(_delay):
        pass

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    resp = await stream_mod.live_log_stream(
        request=fake_req,  # type: ignore[arg-type]
        service_id="test_service",
        _access="read_write",
        registry=_MockHighScaleRegistry(None),  # type: ignore[arg-type]
    )

    agen = resp.body_iterator
    events = []
    async for frame in agen:
        events.append(frame)

    # Even though fetched 3 times, deduplication ensures only 1 event is emitted
    assert len(events) == 1
    parsed = json.loads(events[0]["data"])
    assert parsed["event_id"] == "fixed-id-1"


@pytest.mark.asyncio
async def test_live_log_stream_high_scale_mode(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)

    hs_service = MagicMock()
    sample_rows = [{"event_id": "hs-1", "timestamp": "2026-10-16T12:00:00Z", "url": "/hs"}]

    def mock_hs_fetch(service, limit=25, search=None, filters=None, mask_ips=False):
        assert service == hs_service
        return sample_rows

    monkeypatch.setattr(stream_mod, "_fetch_high_scale_stream_logs", mock_hs_fetch)

    fake_req = _FakeRequest(disconnect_after_ticks=1)

    async def fast_sleep(_delay):
        pass

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    resp = await stream_mod.live_log_stream(
        request=fake_req,  # type: ignore[arg-type]
        service_id="hs_service",
        _access="read_write",
        registry=_MockHighScaleRegistry(hs_service),  # type: ignore[arg-type]
    )

    agen = resp.body_iterator
    events = []
    async for frame in agen:
        events.append(frame)

    assert len(events) == 1
    parsed = json.loads(events[0]["data"])
    assert parsed["event_id"] == "hs-1"
    assert parsed["url"] == "/hs"


@pytest.mark.asyncio
async def test_live_log_stream_analyst_ip_masking(monkeypatch):
    mock_source = {"service_id": "test_service"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: mock_source)

    sample_rows = [{"timestamp": "2026-10-16T12:00:00Z", "client_ip": "192.0.2.123", "url": "/analyst"}]
    monkeypatch.setattr(stream_mod, "_fetch_standard_stream_logs", lambda *a, **k: sample_rows)

    mock_session = MagicMock()
    mock_session.pii_policy = {"mask_ips": True}

    fake_req = _FakeRequest(disconnect_after_ticks=1, analyst_session=mock_session)

    async def fast_sleep(_delay):
        pass

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    resp = await stream_mod.live_log_stream(
        request=fake_req,  # type: ignore[arg-type]
        service_id="test_service",
        _access="read_write",
        registry=_MockHighScaleRegistry(None),  # type: ignore[arg-type]
    )

    agen = resp.body_iterator
    events = []
    async for frame in agen:
        events.append(frame)

    assert len(events) == 1
    parsed = json.loads(events[0]["data"])
    assert parsed["client_ip"] != "192.0.2.123"
    assert parsed["attributes"]["client_ip"] != "192.0.2.123"
