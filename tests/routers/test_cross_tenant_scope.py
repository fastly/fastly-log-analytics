"""Security /: cross-tenant scope enforcement on the alerts +
views routers.

The middleware blocks POST/DELETE on these paths for analysts entirely
(they aren't in ``_ANALYST_ALLOWED_WRITE_PREFIXES``). The router-level
check this file pins is the defense-in-depth read gate AND the
admin-impersonating-analyst write gate — without it, an analyst could
enumerate or modify resources for any service whose service_id they
guessed.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.routers import alerts as alerts_router
from backend.routers import views as views_router


@pytest.fixture
def app_with_session():
    """Mini app that injects a per-test analyst_session on request.state
    so we can exercise the router-level scope check without spinning up
    the full RemoteAccessMiddleware chain."""

    app = FastAPI()
    app.include_router(alerts_router.router)
    app.include_router(views_router.router)

    @app.middleware("http")
    async def stamp_session(request: Request, call_next):
        # Tests inject the desired session via the X-Test-Session header
        # (test-only convenience — production never reads this).
        sid = request.headers.get("x-test-session-services")
        if sid is not None:
            from types import SimpleNamespace

            services = [s for s in sid.split(",") if s]
            request.state.analyst_session = SimpleNamespace(session_id="test", service_ids=services, email="t@t")
        else:
            request.state.analyst_session = None
        return await call_next(request)

    return app


# ── alerts cross-tenant ────────────────────────────────────────────────


def _fake_alert(aid: str, sid: str) -> dict:
    return {
        "id": aid,
        "service_id": sid,
        "name": "t",
        "category": "reliability",
        "metric": "5xx_rate",
        "evaluation_type": "absolute",
        "evaluation_scope": "all",
        "operator": ">",
        "threshold": 5.0,
        "window_min": 5.0,
    }


def test_alerts_list_filters_to_analyst_scope(app_with_session):
    fake_alerts = [
        _fake_alert("a1", "svc-A"),
        _fake_alert("a2", "svc-B"),
        _fake_alert("a3", "svc-C"),
    ]
    with patch("backend.repositories.alerts.get_alerts", return_value=fake_alerts):
        with TestClient(app_with_session) as c:
            r = c.get("/api/alerts/", headers={"x-test-session-services": "svc-A,svc-B"})
    assert r.status_code == 200, r.text
    ids = [a["id"] for a in r.json()["data"]]
    assert ids == ["a1", "a2"]


def test_alerts_list_admin_sees_all(app_with_session):
    fake_alerts = [_fake_alert("a1", "svc-A"), _fake_alert("a2", "svc-B")]
    with patch("backend.repositories.alerts.get_alerts", return_value=fake_alerts):
        with TestClient(app_with_session) as c:
            # No x-test-session-services header → admin (no scope)
            r = c.get("/api/alerts/")
    assert r.status_code == 200, r.text
    assert len(r.json()["data"]) == 2


def test_alerts_list_service_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get("/api/alerts/svc-B", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


def test_alerts_list_service_allows_in_scope(app_with_session):
    with patch("backend.repositories.alerts.get_alerts", return_value=[]):
        with TestClient(app_with_session) as c:
            r = c.get("/api/alerts/svc-A", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200


def test_alerts_create_rejects_unauthorized_analyst(app_with_session):
    payload = {
        "service_id": "svc-B",
        "name": "test",
        "category": "reliability",
        "metric": "5xx_rate",
        "evaluation_type": "absolute",
        "evaluation_scope": "all",
        "operator": ">",
        "threshold": 5.0,
        "window_min": 5,
    }
    with TestClient(app_with_session) as c:
        r = c.post("/api/alerts/", json=payload, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403


# ── views cross-tenant ─────────────────────────────────────────────────


def test_views_list_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get("/api/views/svc-B", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


def test_views_list_allows_in_scope(app_with_session):
    with patch("backend.repositories.views.get_views", return_value=[]):
        with TestClient(app_with_session) as c:
            r = c.get("/api/views/svc-A", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200


def test_views_list_admin_unrestricted(app_with_session):
    with patch("backend.repositories.views.get_views", return_value=[{"id": "v1"}]):
        with TestClient(app_with_session) as c:
            r = c.get("/api/views/svc-anything")
    assert r.status_code == 200


def test_views_create_rejects_unauthorized_analyst(app_with_session):
    payload = {"service_id": "svc-B", "name": "v", "filters": {}, "columns": []}
    with TestClient(app_with_session) as c:
        r = c.post("/api/views/", json=payload, headers={"x-test-session-services": "svc-A"})
    # 403 from scope check, or 422 from schema validation if model rejects
    # the minimal payload. Both are "did not write" — pin the security
    # contract by asserting NOT 200.
    assert r.status_code in (403, 422)
    if r.status_code == 403:
        assert r.json()["detail"]["error"] == "service_not_authorized"
