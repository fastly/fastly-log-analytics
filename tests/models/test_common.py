from __future__ import annotations

from backend.models.common import BaseResponse, DebugCall, DebugQuery


def test_base_response_telemetry_redaction_by_alias_true(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "")  # Disabled

    resp = BaseResponse(
        debug_queries=[DebugQuery(sql="SELECT 1", time_ms=1.5)],
        debug_calls=[DebugCall(service="s3", method="GET", path="foo", time_ms=2.0)],
        is_cached=True,
    )

    # Serialize with by_alias=True
    data = resp.model_dump(by_alias=True)
    assert "_debug_queries" not in data
    assert "_debug_calls" not in data
    assert "debug_queries" not in data
    assert "debug_calls" not in data
    assert data["_is_cached"] is True


def test_base_response_telemetry_redaction_by_alias_false(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "")  # Disabled

    resp = BaseResponse(
        debug_queries=[DebugQuery(sql="SELECT 1", time_ms=1.5)],
        debug_calls=[DebugCall(service="s3", method="GET", path="foo", time_ms=2.0)],
        is_cached=True,
    )

    # Serialize with by_alias=False
    data = resp.model_dump(by_alias=False)
    assert "debug_queries" not in data
    assert "debug_calls" not in data
    assert "_debug_queries" not in data
    assert "_debug_calls" not in data
    assert data["is_cached"] is True


def test_base_response_telemetry_preserved_when_enabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")  # Enabled

    resp = BaseResponse(
        debug_queries=[DebugQuery(sql="SELECT 1", time_ms=1.5)],
        debug_calls=[DebugCall(service="s3", method="GET", path="foo", time_ms=2.0)],
        is_cached=True,
    )

    # Check by_alias=True
    data_alias = resp.model_dump(by_alias=True)
    assert "_debug_queries" in data_alias
    assert "_debug_calls" in data_alias

    # Check by_alias=False
    data_no_alias = resp.model_dump(by_alias=False)
    assert "debug_queries" in data_no_alias
    assert "debug_calls" in data_no_alias
