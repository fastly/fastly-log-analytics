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


def test_base_response_debug_sqlite_redacted_when_disabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "")  # Disabled

    resp = BaseResponse(debug_sqlite=[{"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}])

    data_alias = resp.model_dump(by_alias=True)
    assert "_debug_sqlite" not in data_alias
    assert "debug_sqlite" not in data_alias
    data_no_alias = resp.model_dump(by_alias=False)
    assert "debug_sqlite" not in data_no_alias
    assert "_debug_sqlite" not in data_no_alias


def test_base_response_debug_sqlite_preserved_when_enabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")  # Enabled

    resp = BaseResponse(debug_sqlite=[{"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}])

    assert resp.model_dump(by_alias=True)["_debug_sqlite"] == [{"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}]
    assert "debug_sqlite" in resp.model_dump(by_alias=False)


def test_with_telemetry_pulls_sqlite_collector(monkeypatch):
    """with_telemetry must snapshot the request-scoped SQLite collector —
    and copy it (get_tracked_calls can append a usage_log SELECT to the
    live list after the snapshot)."""
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    from backend.utils import telemetry

    telemetry.start_call_tracking()
    telemetry.record_sqlite_query({"seq": 9, "sql": "SELECT x FROM t", "time_ms": 1.2})
    try:
        resp = BaseResponse.with_telemetry()
        assert resp.debug_sqlite == [{"seq": 9, "sql": "SELECT x FROM t", "time_ms": 1.2}]
        # Snapshot is a copy — later collector appends don't mutate it.
        telemetry.record_sqlite_query({"seq": 10, "sql": "SELECT y", "time_ms": 0.1})
        assert len(resp.debug_sqlite) == 1
    finally:
        telemetry._SQLITE_QUERIES.set(None)
