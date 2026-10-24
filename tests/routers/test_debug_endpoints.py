from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import starlette.datastructures
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.core.share_db.settings import get_setting, set_setting
from backend.main import app
from backend.utils import sqlite_profiler, telemetry
from tests.conftest import MOCK_SERVICE_ID

# ── From test_debug.py ──

"""Tests for the /api/debug router (SQLite ring-buffer surface)."""


@pytest.fixture(autouse=True)
def _isolated_buffer():
    sqlite_profiler.clear()
    yield
    sqlite_profiler.clear()


def test_recent_sqlite_returns_empty_shape_when_buffer_empty():
    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 200
    body = res.json()
    assert body["queries"] == []
    assert body["buffer_size"] == 0
    assert body["buffer_cap"] > 0
    assert body["dropped"] == 0
    assert body["last_seq"] == 0


def test_recent_sqlite_surfaces_real_metadata_db_traffic():
    """Hitting any endpoint that touches metadata_db must populate the
    captured-statements ring buffer. This is the end-to-end contract the
    Debug Panel relies on."""
    from backend.core import metadata as metadata_db

    con = metadata_db.get_con("debug-test-svc")
    con.execute("SELECT 1")
    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 200
    body = res.json()
    assert body["buffer_size"] > 0
    sqls = [q["sql"] for q in body["queries"]]
    assert any("SELECT 1" in s for s in sqls)


def test_recent_sqlite_since_seq_filters():
    from backend.core import metadata as metadata_db

    con1 = metadata_db.get_con("svc-a")
    con1.execute("SELECT 1")
    client = TestClient(app)
    first = client.get("/api/debug/recent-sqlite").json()
    midpoint_seq = first["last_seq"]
    con2 = metadata_db.get_con("svc-b")
    con2.execute("SELECT 2")
    second = client.get(f"/api/debug/recent-sqlite?since_seq={midpoint_seq}").json()
    # Every returned entry must have seq > midpoint.
    assert all(q["seq"] > midpoint_seq for q in second["queries"])


def test_clear_sqlite_drains_buffer():
    from backend.core import metadata as metadata_db

    con = metadata_db.get_con("svc-clear-test")
    con.execute("SELECT 1")
    client = TestClient(app)
    assert client.get("/api/debug/recent-sqlite").json()["buffer_size"] > 0
    res = client.post("/api/debug/clear-sqlite")
    assert res.status_code == 200
    assert res.json()["ok"] is True
    # Buffer must be empty immediately after the POST returns.
    assert sqlite_profiler.get_recent()["buffer_size"] == 0


def test_recent_sqlite_respects_limit():
    from backend.core import metadata as metadata_db

    con = metadata_db.get_con("svc-limit")
    con.execute("SELECT 1")
    con.execute("SELECT 2")
    con.execute("SELECT 3")
    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite?limit=2")
    body = res.json()
    assert len(body["queries"]) <= 2


# ── From test_debug_envelope_gating.py ──

pytestmark = pytest.mark.security_regression


_CALLS = [{"service": "Fastly API", "method": "GET", "path": "/probe", "time_ms": 1.0}]


@pytest.fixture
def recorded_calls():
    with patch.object(telemetry, "get_tracked_calls", return_value=_CALLS):
        yield


def _debug_keys(body: dict) -> list[str]:
    return [k for k in body if k.startswith(("_debug", "debug_"))]


def test_gated_envelope_adds_calls_only_when_debug_responses_is_on(monkeypatch, recorded_calls):
    monkeypatch.setenv("DEBUG_RESPONSES", "true")

    out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)

    assert out == {"ok": True, "_debug_calls": _CALLS}


def test_gated_envelope_adds_nothing_when_debug_responses_is_off(monkeypatch, recorded_calls):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

    out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)

    assert out == {"ok": True}


def test_gated_envelope_drops_a_stale_envelope_when_debug_responses_is_off(monkeypatch, recorded_calls):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

    out = telemetry.debug_envelope(
        {"ok": True, "_debug_calls": ["stale"], "_debug_queries": ["stale"]},
        gated=True,
        is_cached=None,
        inject_calls=True,
    )

    assert out == {"ok": True}


def test_gated_envelope_without_inject_calls_still_never_injects(monkeypatch, recorded_calls):
    monkeypatch.setenv("DEBUG_RESPONSES", "true")

    out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=True)

    assert out == {"ok": True, "_is_cached": True}


def _validate(client):
    def _fastly_spy(method, path, **kwargs):
        return {"id": "tok-1", "name": "T", "user_id": "u"} if "/tokens/self" in path else {"name": "Svc"}

    with patch("backend.core.fastly.client.fastly", side_effect=_fastly_spy):
        return client.post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})


def _check_domain(client):
    with patch("backend.routers.provision._check_domain_available", return_value=(True, None)):
        return client.get("/api/provision/check-domain?prefix=valid-prefix-1")


def _check_fos_ok(client):
    fake = MagicMock()
    fake.list_objects_v2.return_value = {"Contents": []}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _check_fos_failed(client):
    fake = MagicMock()
    fake.list_objects_v2.side_effect = RuntimeError("boom")
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _audit_logs(client):
    return client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _iceberg_calendar(client):
    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: {"service_id": "service123", "name": "service123"}
    with patch(
        "backend.core.iceberg.get_snapshot_calendar", return_value={"2026-08-19": {"data_files": 5, "size_bytes": 1}}
    ):
        return client.get("/api/admin/iceberg-calendar?table_name=logs")


def _log_fields_get(client):
    svcconfig.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "log_fields": {"groups": ["A"]}})
    return client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _generate_viewer_key(client):
    invite = {
        "service_id": "svc-1",
        "name": "MyService",
        "access_key_id": "AK",
        "secret_key": "SK",
        "fos_bucket": "b",
        "fos_region": "us-east-1",
        "fos_endpoint": "https://s3.us-east-1.amazonaws.com",
        "fos_prefix": "",
    }
    with patch("backend.provision.generate_analyst_invite", return_value=invite):
        return client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")


_SITES = [
    pytest.param(_validate, id="provision-validate"),
    pytest.param(_check_domain, id="provision-check-domain"),
    pytest.param(_check_fos_ok, id="provision-check-fos-ok"),
    pytest.param(_check_fos_failed, id="provision-check-fos-failed"),
    pytest.param(_audit_logs, id="audit-logs"),
    pytest.param(_iceberg_calendar, id="iceberg-calendar"),
    pytest.param(_log_fields_get, id="service-log-fields"),
    pytest.param(_generate_viewer_key, id="generate-viewer-key"),
]


@pytest.mark.parametrize("call", _SITES)
def test_route_emits_no_debug_envelope_when_debug_responses_is_off(client, monkeypatch, call):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

    r = call(client)

    assert r.status_code == 200
    assert _debug_keys(r.json()) == []


@pytest.mark.parametrize("call", _SITES)
def test_route_emits_the_call_trace_when_debug_responses_is_on(client, monkeypatch, recorded_calls, call):
    monkeypatch.setenv("DEBUG_RESPONSES", "true")

    r = call(client)

    assert r.status_code == 200
    assert [c["path"] for c in r.json()["_debug_calls"]] == ["/probe"]


# ── From test_debug_settings.py ──

_is_remote_flag = False


original_getattr = starlette.datastructures.State.__getattr__


def mock_state_getattr(self, key: str):
    if key == "is_remote":
        return _is_remote_flag
    try:
        return original_getattr(self, key)
    except (AttributeError, KeyError):
        raise AttributeError(f"'State' object has no attribute '{key}'")


@pytest.fixture(autouse=True)
def patch_state(monkeypatch):
    global _is_remote_flag
    _is_remote_flag = False
    monkeypatch.setattr(starlette.datastructures.State, "__getattr__", mock_state_getattr)
    yield
    _is_remote_flag = False


def test_get_and_patch_debug_settings():
    global _is_remote_flag
    _is_remote_flag = False
    client = TestClient(app)

    # 1. Fetch initial settings (should default to disabled)
    res = client.get("/api/admin/debug-settings")
    assert res.status_code == 200
    body = res.json()
    assert body["query_debug_visibility"] == "disabled"
    assert body["api_call_debug_visibility"] == "disabled"

    # 2. Update settings via PATCH
    patch_res = client.patch(
        "/api/admin/debug-settings",
        json={
            "query_debug_visibility": "both",
            "api_call_debug_visibility": "analysts",
        },
    )
    assert patch_res.status_code == 200
    patched_body = patch_res.json()
    assert patched_body["query_debug_visibility"] == "both"
    assert patched_body["api_call_debug_visibility"] == "analysts"

    # Verify database settings were updated
    assert get_setting("query_debug_visibility") == "both"
    assert get_setting("api_call_debug_visibility") == "analysts"

    # Clean up settings
    set_setting("query_debug_visibility", "disabled")
    set_setting("api_call_debug_visibility", "disabled")


def test_recent_sqlite_is_remote_blocked():
    global _is_remote_flag
    _is_remote_flag = True
    set_setting("query_debug_visibility", "disabled")

    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 403
    detail = res.json()["detail"]
    assert detail["error"] == "sql_debug_disabled"
    assert "SQL debug is disabled for analysts" in detail["message"]


def test_recent_sqlite_is_remote_allowed_when_visibility_both():
    global _is_remote_flag
    _is_remote_flag = True
    set_setting("query_debug_visibility", "both")

    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 200

    set_setting("query_debug_visibility", "disabled")


def test_clear_sqlite_is_remote_always_blocked():
    global _is_remote_flag
    _is_remote_flag = True

    client = TestClient(app)
    res = client.post("/api/debug/clear-sqlite")
    assert res.status_code == 403
    detail = res.json()["detail"]
    assert detail["error"] == "admin_only"
    assert "Only administrators can clear the SQLite debug buffer" in detail["message"]
