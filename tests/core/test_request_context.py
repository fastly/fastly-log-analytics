"""Tests for `backend.core.request_context`.

Phase 2 coverage: the new RequestContext dependency, the inline tenancy
enforcement, and the structural read_only invariant.

Every tenancy / scope assertion is tagged ``security_regression`` because
the whole point of Phase 2 is to make the existing audit-finding 003-class
guarantees a structural invariant rather than a per-route discipline.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.core import request_context as rc
from backend.core.request_context import RequestContext, _enforce_service_access

# ── Tenancy enforcement (security_regression — Phase 2.8 requirement) ─────────

pytestmark_for_tenancy = pytest.mark.security_regression


@pytest.mark.security_regression
def test_admin_request_with_service_id_passes():
    request = MagicMock()
    request.state = SimpleNamespace(analyst_session=None)
    assert _enforce_service_access(request, "svc-123") == "svc-123"


@pytest.mark.security_regression
def test_admin_request_without_service_id_raises_400():
    request = MagicMock()
    request.state = SimpleNamespace(analyst_session=None)
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, None)
    assert exc.value.status_code == 400
    assert exc.value.detail["error"] == "no_service"


@pytest.mark.security_regression
def test_scoped_analyst_with_authorized_service_passes():
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=["svc-1", "svc-2"]),
    )
    assert _enforce_service_access(request, "svc-1") == "svc-1"


@pytest.mark.security_regression
def test_scoped_analyst_with_unauthorized_service_raises_403():
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=["svc-1"]),
    )
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, "svc-other")
    assert exc.value.status_code == 403
    assert exc.value.detail["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_scoped_analyst_without_service_id_defaults_to_first_allowed():
    """Mirrors require_service_access semantics — an analyst calling a
    route that didn't pass an explicit service falls back to the first
    of their scoped services."""
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=["svc-7", "svc-8"]),
    )
    # set() order is not guaranteed; assert the result is one of allowed.
    out = _enforce_service_access(request, None)
    assert out in {"svc-7", "svc-8"}


@pytest.mark.security_regression
def test_scoped_analyst_with_empty_invite_raises_400():
    """An analyst session with no allowed services has nothing to default
    to; raise 400 rather than silently letting them through."""
    request = MagicMock()
    request.state = SimpleNamespace(
        analyst_session=SimpleNamespace(service_ids=[]),
    )
    with pytest.raises(HTTPException) as exc:
        _enforce_service_access(request, None)
    assert exc.value.status_code == 400


# ── RequestContext shape ──────────────────────────────────────────────────────


def test_request_context_carries_required_fields():
    """Constructor signature pins ADR-02's required attributes."""
    from backend.core.request_telemetry import RequestTelemetry

    ctx = RequestContext(
        service_id="svc-1",
        source={"name": "svc-1", "endpoint_url": "http://localhost"},
        con=MagicMock(),
        telemetry=RequestTelemetry("GET", "/api/x"),
        analyst_session=None,
    )
    assert ctx.service_id == "svc-1"
    assert ctx.source["name"] == "svc-1"
    assert ctx.con is not None
    assert ctx.telemetry is not None
    assert ctx.analyst_session is None
    assert ctx.cached_temps == {}
    assert ctx.read_only is True


def test_cached_temps_are_per_instance():
    """Default factory yields a fresh dict per context — first repo's
    insertion can't leak to a later request's context."""
    from backend.core.request_telemetry import RequestTelemetry

    a = RequestContext(
        service_id="svc",
        source={"name": "svc"},
        con=MagicMock(),
        telemetry=RequestTelemetry("GET", "/"),
    )
    b = RequestContext(
        service_id="svc",
        source={"name": "svc"},
        con=MagicMock(),
        telemetry=RequestTelemetry("GET", "/"),
    )
    a.cached_temps["window:1h"] = "tmp_1234"
    assert "window:1h" not in b.cached_temps


# ── _resolve_source ───────────────────────────────────────────────────────────


def test_resolve_source_returns_source_dict_for_known_service():
    fake_source = {"name": "svc-x", "endpoint_url": "http://localhost"}
    with patch("backend.core.duckdb.get_source_for_service", return_value=fake_source):
        out = rc._resolve_source("svc-x")
    assert out is fake_source


def test_resolve_source_raises_400_for_unknown_service():
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        with pytest.raises(HTTPException) as exc:
            rc._resolve_source("svc-missing")
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


# ── End-to-end via FastAPI TestClient (structural invariant pin) ──────────────


def _make_app_with_ctx_route() -> FastAPI:
    """Mini app that consumes RequestContext via a dependency. Used to
    pin the end-to-end shape — the route can't be reached without
    construction running tenancy enforcement first."""
    from fastapi import Depends
    from fastapi import Request as FRequest

    app = FastAPI()

    fake_source = {"name": "svc-1", "endpoint_url": "http://localhost"}

    @app.middleware("http")
    async def install_session(request: FRequest, call_next):
        # Test-only convenience header: x-test-session-services=svc-1,svc-2
        # → analyst_session with those service_ids
        sid_header = request.headers.get("x-test-session-services")
        if sid_header is not None:
            request.state.analyst_session = SimpleNamespace(
                service_ids=[s.strip() for s in sid_header.split(",") if s.strip()],
            )
        else:
            request.state.analyst_session = None
        return await call_next(request)

    @app.get("/api/test")
    def route(ctx: RequestContext = Depends(rc.build_request_context)):
        return {
            "service_id": ctx.service_id,
            "read_only": ctx.read_only,
            "is_admin": ctx.analyst_session is None,
        }

    # Patch service resolution + connection bridges so we don't need a
    # real DuckDB pool stood up for the TestClient.
    app.state._patches = []
    app.state._fake_source = fake_source
    return app


@pytest.mark.security_regression
def test_route_admin_request_with_explicit_service_succeeds(monkeypatch):
    app = _make_app_with_ctx_route()

    with (
        patch("backend.core.request_context._resolve_source", return_value=app.state._fake_source),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        client = TestClient(app)
        r = client.get("/api/test", params={"service": "svc-1"})

    assert r.status_code == 200
    body = r.json()
    assert body["service_id"] == "svc-1"
    assert body["read_only"] is True
    assert body["is_admin"] is True


@pytest.mark.security_regression
def test_route_analyst_request_with_unauthorized_service_403s(monkeypatch):
    """The structural Phase 2 invariant: there's no way to reach the
    route body for a service the analyst doesn't own."""
    app = _make_app_with_ctx_route()

    with (
        patch("backend.core.request_context._resolve_source", return_value=app.state._fake_source),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        client = TestClient(app)
        r = client.get(
            "/api/test",
            params={"service": "svc-OTHER"},
            headers={"x-test-session-services": "svc-1"},
        )

    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_route_read_only_cannot_be_overridden_by_query_param(monkeypatch):
    """The whole reason `read_only` is a constructor arg and NOT a dep
    param: an attacker passing ?read_only=false must NOT flip the
    in-flight connection mode. Verified by sending the bait and
    confirming the route still reports read_only=True."""
    app = _make_app_with_ctx_route()

    with (
        patch("backend.core.request_context._resolve_source", return_value=app.state._fake_source),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        client = TestClient(app)
        r = client.get(
            "/api/test",
            params={"service": "svc-1", "read_only": "false"},
        )

    assert r.status_code == 200
    assert r.json()["read_only"] is True
