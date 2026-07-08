"""Control Room router — Phase 0 stub tests.

Covers:
- Every tab stub GET returns 200 for admin and analyst sessions
- Mutation endpoints return 501 for admin, 403 for analyst
- Unknown tab returns 404
- SSE heartbeat: connect, receive ≥3 metrics_tick events, disconnect cleanly
- SSE event schema validation (event_schema_version: 1)
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


@pytest.mark.parametrize("tab", ALL_TABS)
def test_tab_get_admin_200(admin_client, tab):
    """Admin can read every tab stub — 200 with canned data."""
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab
    assert data["status"] == "stub"


# ── Tab GET tests (analyst) ──────────────────────────────────────────────────


@pytest.mark.parametrize("tab", ALL_TABS)
def test_tab_get_analyst_200(analyst_client, tab):
    """Analyst can read every tab stub — 200 with canned data."""
    resp = analyst_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab
    assert data["status"] == "stub"


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


# ── SSE heartbeat ────────────────────────────────────────────────────────────


class _FakeRequest:
    """Minimal Request stand-in for direct-drive SSE tests."""

    def __init__(self, disconnect_after: int = 999):
        self._count = 0
        self._disconnect_after = disconnect_after
        self.state = SimpleNamespace(analyst_session=None)

    async def is_disconnected(self) -> bool:
        return self._count >= self._disconnect_after


@pytest.mark.asyncio
async def test_sse_heartbeat(monkeypatch):
    """Connect to the realtime stream, receive ≥3 metrics_tick events,
    verify schema, then disconnect cleanly."""
    import backend.routers.control_room as cr_mod

    _real_sleep = asyncio.sleep

    async def _fast_sleep(_seconds):
        await _real_sleep(0)

    monkeypatch.setattr(cr_mod.asyncio, "sleep", _fast_sleep)

    resp = await cr_mod.realtime_stream(
        request=_FakeRequest(),
        service_id=MOCK_SERVICE_ID,
        _access=MOCK_SERVICE_ID,
    )

    agen = resp.body_iterator
    frames: list[dict] = []
    try:
        for _ in range(3):
            raw = await asyncio.wait_for(agen.__anext__(), timeout=5)
            frame = json.loads(raw)
            frames.append(frame)
    finally:
        await agen.aclose()

    assert len(frames) >= 3
    for frame in frames:
        assert frame["event"] == "metrics_tick"
        assert frame["event_schema_version"] == 1
        assert "timestamp" in frame
        assert "data" in frame
        assert isinstance(frame["data"], dict)
        assert "requests_per_second" in frame["data"]


@pytest.mark.asyncio
async def test_sse_event_schema_v1(monkeypatch):
    """Every emitted frame carries event_schema_version: 1 and the expected
    data shape so the frontend can assert on the wire format."""
    import backend.routers.control_room as cr_mod

    _real_sleep = asyncio.sleep

    async def _fast_sleep(_seconds):
        await _real_sleep(0)

    monkeypatch.setattr(cr_mod.asyncio, "sleep", _fast_sleep)

    resp = await cr_mod.realtime_stream(
        request=_FakeRequest(),
        service_id=MOCK_SERVICE_ID,
        _access=MOCK_SERVICE_ID,
    )

    agen = resp.body_iterator
    try:
        raw = await asyncio.wait_for(agen.__anext__(), timeout=5)
        frame = json.loads(raw)
    finally:
        await agen.aclose()

    assert frame["event_schema_version"] == 1
    expected_data_keys = {"requests_per_second", "error_rate", "cache_hit_ratio", "bandwidth_mbps"}
    assert expected_data_keys == set(frame["data"].keys())


@pytest.mark.asyncio
async def test_sse_disconnects_cleanly(monkeypatch):
    """Stream stops when the client disconnects (is_disconnected returns True)."""
    import backend.routers.control_room as cr_mod

    _real_sleep = asyncio.sleep

    async def _fast_sleep(_seconds):
        await _real_sleep(0)

    monkeypatch.setattr(cr_mod.asyncio, "sleep", _fast_sleep)

    req = _FakeRequest(disconnect_after=2)

    resp = await cr_mod.realtime_stream(
        request=req,
        service_id=MOCK_SERVICE_ID,
        _access=MOCK_SERVICE_ID,
    )

    agen = resp.body_iterator
    frames: list[dict] = []
    try:
        async for raw in agen:
            frames.append(json.loads(raw))
            req._count += 1
    except StopAsyncIteration:
        pass
    finally:
        await agen.aclose()

    assert len(frames) <= 3
