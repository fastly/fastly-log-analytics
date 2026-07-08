"""Control Room router — Phase 1 tests.

Covers:
- Every tab stub GET returns 200 for admin and analyst sessions
- Mutation endpoints return 501 for admin, 403 for analyst
- Unknown tab returns 404
- SSE stream: publisher-driven metrics_tick events
- SSE event schema validation (event_schema_version: 1)
- Log-field-audit endpoint
- Correlator endpoint
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.deps import require_admin, require_service_access
from backend.main import app
from tests.conftest import MOCK_SERVICE_ID

# ── Tab names ────────────────────────────────────────────────────────────────

ALL_TABS = [
    "overview",
    "performance",
    "origin",
    "security",
    "network",
    "sessions",
    "cost",
    "insights",
    "admin_health",
]

LIVE_TABS = ["overview", "cost"]
STUB_TABS = [t for t in ALL_TABS if t not in LIVE_TABS]

MUTATION_ENDPOINTS = [
    "mitigations",
    "rules",
    "allowlist",
    "big-red-button",
    "cost-governor",
]

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def admin_client(in_memory_duckdb, test_service_source):
    """TestClient with admin overrides — require_admin passes."""
    from backend.deps import get_con, get_service_id

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]
    app.dependency_overrides[require_service_access] = lambda: MOCK_SERVICE_ID
    app.dependency_overrides[require_admin] = lambda: None

    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def analyst_client(in_memory_duckdb, test_service_source):
    """TestClient with analyst overrides — require_admin rejects."""
    from backend.deps import get_con, get_service_id

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]
    app.dependency_overrides[require_service_access] = lambda: MOCK_SERVICE_ID

    def _analyst_require_admin():
        raise HTTPException(
            status_code=403,
            detail={"error": "admin_only", "message": "This operation requires admin access."},
        )

    app.dependency_overrides[require_admin] = _analyst_require_admin

    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ── Tab GET tests (admin) ────────────────────────────────────────────────────


@pytest.mark.parametrize("tab", STUB_TABS)
def test_tab_get_admin_stub_200(admin_client, tab):
    """Admin can read every stub tab — 200 with canned data."""
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab
    assert data["status"] == "stub"


@pytest.mark.parametrize("tab", LIVE_TABS)
def test_tab_get_admin_live_200(admin_client, tab):
    """Admin can read live tabs — 200 with status=live."""
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab
    assert data["status"] == "live"


# ── Tab GET tests (analyst) ──────────────────────────────────────────────────


@pytest.mark.parametrize("tab", ALL_TABS)
def test_tab_get_analyst_200(analyst_client, tab):
    """Analyst can read every tab — 200 with canned data."""
    resp = analyst_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab


# ── Unknown tab ──────────────────────────────────────────────────────────────


def test_unknown_tab_404(admin_client):
    """Unknown tab name returns 404."""
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/bogus",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "unknown_tab"


# ── Mutation tests (admin → 501) ─────────────────────────────────────────────


@pytest.mark.parametrize("endpoint", MUTATION_ENDPOINTS)
def test_mutation_admin_501(admin_client, endpoint):
    """Admin mutation stubs return 501 Not Implemented."""
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{endpoint}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"action": "test", "payload": {}},
    )
    assert resp.status_code == 501
    assert resp.json()["detail"]["error"] == "not_implemented"


# ── Mutation tests (analyst → 403) ───────────────────────────────────────────


@pytest.mark.parametrize("endpoint", MUTATION_ENDPOINTS)
def test_mutation_analyst_403(analyst_client, endpoint):
    """Analyst mutation calls return 403 — admin-only."""
    resp = analyst_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{endpoint}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"action": "test", "payload": {}},
    )
    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "admin_only"


# ── SSE real-time stream ────────────────────────────────────────────────────


class _FakeRequest:
    """Minimal Request stand-in for direct-drive SSE tests."""

    def __init__(self, disconnect_after: int = 999):
        self._count = 0
        self._disconnect_after = disconnect_after
        self.state = SimpleNamespace(analyst_session=None)

    async def is_disconnected(self) -> bool:
        self._count += 1
        return self._count >= self._disconnect_after


async def _drive_sse_with_payload(monkeypatch, payload: dict) -> dict:
    """Helper: start SSE stream, publish a payload, return the first frame."""
    import backend.routers.control_room as cr_mod
    from backend.core.realtime.publisher import RealtimeMetricsPublisher

    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())

    from backend.core.realtime import poller as poller_mod

    monkeypatch.setattr(poller_mod.poller, "ensure_polling", lambda sid: None)
    monkeypatch.setattr("backend.core.realtime.publisher.publisher", test_publisher)

    req = _FakeRequest(disconnect_after=10)

    resp = await cr_mod.realtime_stream(
        request=req,
        service_id=MOCK_SERVICE_ID,
        _access=MOCK_SERVICE_ID,
    )

    agen = resp.body_iterator

    async def _delayed_publish():
        await asyncio.sleep(0.05)
        test_publisher.publish(MOCK_SERVICE_ID, payload)

    asyncio.ensure_future(_delayed_publish())

    try:
        raw = await asyncio.wait_for(agen.__anext__(), timeout=5)
        return json.loads(raw)
    finally:
        await agen.aclose()


@pytest.mark.asyncio
async def test_sse_real_metrics_shape(monkeypatch):
    """Publisher-driven SSE emits metrics_tick with the full data shape."""
    frame = await _drive_sse_with_payload(
        monkeypatch,
        {
            "event": "metrics_tick",
            "event_schema_version": 1,
            "timestamp": "2026-07-07T00:00:00+00:00",
            "status": "ok",
            "data": {
                "requests_per_second": 42.5,
                "error_rate": 0.01,
                "cache_hit_ratio": 0.95,
                "bandwidth_mbps": 1.5,
                "status_breakdown": {"status_2xx": 100},
                "estimated_cost_usd": 0.001,
            },
            "aggregate_delay": 3,
        },
    )

    assert frame["event"] == "metrics_tick"
    assert frame["event_schema_version"] == 1
    assert frame["status"] == "ok"
    assert frame["data"]["requests_per_second"] == 42.5
    assert "cache_hit_ratio" in frame["data"]
    assert "bandwidth_mbps" in frame["data"]
    assert "estimated_cost_usd" in frame["data"]


@pytest.mark.asyncio
async def test_sse_rt_down_payload(monkeypatch):
    """When RT is down, the SSE payload carries status='rt_down'."""
    from backend.core.realtime.transform import error_tick_payload

    frame = await _drive_sse_with_payload(monkeypatch, error_tick_payload())

    assert frame["status"] == "rt_down"
    assert frame["data"]["requests_per_second"] == 0


# ── Log-field-audit ─────────────────────────────────────────────────────────


def test_log_field_audit_200(admin_client, monkeypatch):
    """Log-field-audit returns enabled fields and feature status."""
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"log_fields": None})

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/log-field-audit",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["service_id"] == MOCK_SERVICE_ID
    assert isinstance(data["enabled_fields"], list)
    assert isinstance(data["features"], list)
    assert len(data["features"]) > 0
    for feature in data["features"]:
        assert "name" in feature
        assert "status" in feature
        assert feature["status"] in ("ok", "missing_fields")


def test_log_field_audit_missing_fields(admin_client, monkeypatch):
    """When config has no groups, some features should report missing fields."""
    from backend import config as svcconfig

    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda sid: {"log_fields": {"groups": [], "field_overrides": {}}},
    )

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/log-field-audit",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    missing_features = [f for f in data["features"] if f["status"] == "missing_fields"]
    assert len(missing_features) > 0


# ── Correlator ──────────────────────────────────────────────────────────────


def test_correlate_unknown_dimension_400(admin_client):
    """Unknown dimension returns 400."""
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"dimension": "totally_bogus_field"},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "unknown_dimension"


# ── OpenAPI contract ────────────────────────────────────────────────────────


def test_openapi_has_control_room_endpoints():
    """Verify key control-room endpoints are registered in the OpenAPI schema."""
    schema = app.openapi()
    paths = schema["paths"]

    assert "/api/services/{service_id}/control-room/{tab}" in paths
    assert "/api/services/{service_id}/realtime-stream" in paths
    assert "/api/services/{service_id}/log-field-audit" in paths
    assert "/api/services/{service_id}/control-room/correlate" in paths
    assert "/api/services/{service_id}/control-room/mitigations" in paths
