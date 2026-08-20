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
    "insights",
    "admin_health",
]

LIVE_TABS = ["overview"]
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
            "event_schema_version": 2,
            "timestamp": "2026-07-07T00:00:00+00:00",
            "status": "ok",
            "data": {
                "requests_per_second": 42.5,
                "error_rate": 0.01,
                "cache_hit_ratio": 0.95,
                "bandwidth_mbps": 1.5,
                "status_breakdown": {"status_2xx": 100},
                "estimated_cost_usd": 0.001,
                "origin_requests_per_second": 5.0,
                "origin_bandwidth_mbps": 0.2,
                "shield_requests": 10,
                "shield_hit_ratio": 0.05,
                "pass_requests": 0,
                "synth_requests": 0,
                "waf_blocked": 0,
                "waf_logged": 0,
                "waf_passed": 42,
                "pop_count": 2,
                "degraded_pops": [],
            },
            "aggregate_delay": 3,
        },
    )

    assert frame["event"] == "metrics_tick"
    assert frame["event_schema_version"] == 2
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


# ── Seed, Correlate, & Wizard endpoints ──────────────────────────────────


def test_realtime_seed_with_cached_ticks(admin_client, monkeypatch):
    """If the publisher has enough ticks cached, return them directly."""
    from backend.core.realtime.publisher import publisher as rt_publisher

    dummy_ticks = [{"timestamp": "2026-07-07T00:00:00Z"}] * 60
    monkeypatch.setattr(rt_publisher, "get_recent_ticks", lambda sid, count: dummy_ticks)

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/realtime-seed",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["ticks"]) == 60


def test_realtime_seed_with_fetch_fallback(admin_client, monkeypatch):
    """If the publisher cache is empty, fall back to fetching from RT API."""
    import backend.routers.control_room as cr_mod
    from backend.core.realtime.publisher import publisher as rt_publisher

    monkeypatch.setattr(rt_publisher, "get_recent_ticks", lambda sid, count: [])

    dummy_ticks = [{"timestamp": "2026-07-07T00:01:00Z"}]
    monkeypatch.setattr(cr_mod, "_fetch_seed_ticks", lambda sid: dummy_ticks)
    monkeypatch.setattr("backend.core.realtime.poller.poller.ensure_polling", lambda sid: None)

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/realtime-seed",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["ticks"]) == 1
    assert data["ticks"][0]["timestamp"] == "2026-07-07T00:01:00Z"


def test_correlate_success(admin_client, monkeypatch):
    """Verify correlate returns correct dimension stats with mocked DuckDB connection."""
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    mock_source = {"name": "svc"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: mock_source)

    mock_con = MagicMock()
    mock_con.execute().fetchall.return_value = [("200", 100)]
    mock_con.execute().fetchone.return_value = (datetime(2026, 8, 19, 12, 0, 0, tzinfo=UTC),)
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda source, read_only: mock_con)

    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"dimension": "status", "window_minutes": 10, "limit": 5},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["dimension"] == "status"
    assert data["top"] == [{"value": "200", "count": 100}]
    assert "freshness" in data


def test_correlate_unknown_service_source_404(admin_client, monkeypatch):
    """If service source doesn't exist, return 404."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)

    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"dimension": "status"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "no_data_source"


def test_wizard_state_and_step_flow(admin_client):
    """Verify completion of wizard steps and state query from audit logs."""
    # 1. Check initial empty state
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/state",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    assert resp.json()["completed_steps"] == []
    assert resp.json()["is_complete"] is False

    # 2. Complete a step
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/step",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"step": "domain", "details": {"custom": True}},
    )
    assert resp.status_code == 200
    assert resp.json()["recorded"] is True
    assert resp.json()["step"] == "domain"

    # 3. Complete wizard
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/step",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"step": "complete", "details": {}},
    )
    assert resp.status_code == 200

    # 4. Check updated state
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/state",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    completed = resp.json()["completed_steps"]
    assert "domain" in completed
    assert "complete" in completed
