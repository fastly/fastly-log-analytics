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

import duckdb
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


# ── Client-cancel / GeneratorExit conn-release (POOL-3 regression guard) ──────


def _install_inspectable_pool(monkeypatch, service_key: str):
    """Register a real, inspectable ``_Pool`` for ``service_key`` seeded with
    one idle mock connection, with the pool force-enabled and the iceberg
    view-rebind probe stubbed so a checkout takes the fast (reused-conn) path
    without touching S3/Iceberg. Returns ``(pool, mock_con)``."""
    from backend.core import duckdb_pool

    pool = duckdb_pool._Pool(service_key=service_key, max_size=2)
    mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)
    # release()'s temp-table sweep calls con.execute(...).fetchall(); return
    # no rows so the sweep is a no-op against the mock.
    mock_con.execute.return_value.fetchall.return_value = []
    pool._idle.put_nowait(mock_con)
    pool._in_use = 1

    monkeypatch.setattr(duckdb_pool, "_pool_enabled", lambda: True)
    monkeypatch.setitem(duckdb_pool._pools, service_key, pool)
    # Skip the iceberg-view rebind probe on the reused-conn checkout path.
    monkeypatch.setattr(duckdb_pool._Pool, "_prepare_checkout", lambda self, con, src: con)
    return pool, mock_con


def _drive_request_context_to_yield(service_key: str):
    """Call ``build_request_context`` directly and advance the generator to
    its ``yield ctx`` (the point a real request hands control to the route).
    Returns the live generator so the test can close()/throw() into it."""
    request = MagicMock()
    request.state = SimpleNamespace(analyst_session=None)
    request.method = "GET"
    request.url.path = "/api/test"
    gen = rc.build_request_context(request, service_id=service_key)
    next(gen)  # runs setup + checkout, parks at `yield ctx`
    return gen


def test_client_cancel_returns_connection_to_pool(monkeypatch):
    """POOL-3: a client cancel / request timeout unwinds
    ``build_request_context``'s generator via ``GeneratorExit`` at
    ``yield ctx``. Because ``GeneratorExit`` is a ``BaseException`` (not an
    ``Exception``), ``checkout_connection``'s ``except Exception`` is
    bypassed, ``errored`` stays False, and the connection is RETURNED to the
    idle pool — not discarded. Pins the ``with``/``try``/``finally`` nesting
    so a future refactor can't silently turn a cancel into a leaked slot or
    a needless ~150ms rebuild."""
    pool, mock_con = _install_inspectable_pool(monkeypatch, "svc-cancel")

    with patch(
        "backend.core.request_context._resolve_source",
        return_value={"name": "svc-cancel", "endpoint_url": "http://localhost"},
    ):
        gen = _drive_request_context_to_yield("svc-cancel")
        # Connection is checked out: idle drained, slot still owned.
        assert pool._in_use == 1
        assert pool._idle.qsize() == 0
        # Simulate the client cancel — GeneratorExit raised at `yield ctx`.
        gen.close()

    assert pool._in_use == 1, "slot still accounted (not leaked)"
    assert pool._idle.qsize() == 1, "clean cancel returns the conn to idle"
    assert pool._discarded_total == 0, "clean cancel must not discard a healthy conn"
    mock_con.close.assert_not_called()


def test_real_exception_discards_connection(monkeypatch):
    """Contrast to the cancel path: a genuine error thrown into the generator
    IS an ``Exception``, so ``checkout_connection`` marks the conn errored and
    the pool discards it — a poisoned connection never gets reused. Anchors
    the cancel-path assertion above by showing the discard branch still
    fires for real failures."""
    pool, mock_con = _install_inspectable_pool(monkeypatch, "svc-err")

    with patch(
        "backend.core.request_context._resolve_source",
        return_value={"name": "svc-err", "endpoint_url": "http://localhost"},
    ):
        gen = _drive_request_context_to_yield("svc-err")
        assert pool._in_use == 1
        with pytest.raises(RuntimeError, match="boom"):
            gen.throw(RuntimeError("boom"))

    assert pool._discarded_total == 1, "a real error discards the conn"
    assert pool._in_use == 0, "discard frees the slot"
    assert pool._idle.qsize() == 0
    mock_con.close.assert_called_once()
