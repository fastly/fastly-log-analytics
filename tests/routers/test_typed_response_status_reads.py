"""Typed small-read models (schema, audit-logs, debug state, cmcd status) keep their wire payloads."""

from __future__ import annotations

import datetime as dt

from tests.conftest import MOCK_SERVICE_ID


def _body(resp):
    return {k: v for k, v in resp.json().items() if not k.startswith("_")}


def test_debug_state_matches_producer(client):
    from backend.models.common import _debug_responses_enabled

    resp = client.get("/api/debug/state")
    assert resp.status_code == 200
    assert _body(resp) == {"debug_responses_enabled": _debug_responses_enabled()}


def test_schema_response_serializes_schema_alias():
    from backend.models.status_reads import SchemaResponse

    payload = {"schema": [{"name": "ip", "type": "VARCHAR"}], "table_name": "logs_x"}
    assert SchemaResponse.model_validate(payload).model_dump(mode="json", by_alias=True) == payload


def test_audit_logs_entries_encode_like_jsonable_encoder():
    from backend.models.status_reads import AuditLogsResponse

    payload = {
        "total": 1,
        "page": 1,
        "per_page": 50,
        "entries": [{"timestamp": dt.datetime(2026, 1, 1, tzinfo=dt.UTC), "event_type": "x"}],
        "_debug_calls": [],
    }
    dumped = AuditLogsResponse.model_validate(payload).model_dump(mode="json")
    assert dumped["entries"] == [{"timestamp": "2026-01-01T00:00:00+00:00", "event_type": "x"}]
    assert dumped["_debug_calls"] == []


def test_cmcd_status_disabled_and_enabled_shapes(client, monkeypatch):
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    off = client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")
    assert off.status_code == 200
    assert _body(off) == {"enabled": False}

    cfg = {"cmcd": {"enabled": True, "mode": "headers", "version": 1, "enabled_at": "2026-01-01T00:00:00+00:00"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    on = client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")
    assert _body(on) == {
        "enabled": True,
        "mode": "headers",
        "version": 1,
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }
