"""Analysts never receive a ``_debug_*`` envelope from the admin routes that attach one.

``provision``, ``services/core`` (log-fields, generate-viewer-key), ``services/audit`` and
``admin/iceberg`` build plain-dict responses with a call-trace envelope. Two independent
guards keep it away from analysts: the paths are blocked outright for the analyst role, and
``RemoteAccessMiddleware._strip_analyst_envelope`` removes the envelope keys from anything
that does get through. Both are pinned here, including the Postgres spelling
(``_debug_postgres``) that ``BaseResponse`` emits next to ``_debug_sqlite``.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.utils.remote_access import RemoteAccessMiddleware, _strip_analyst_envelope
from tests.conftest import override_request_context
from tests.remote_access.test_middleware import _login_analyst, _seed_invite, _start_share

pytestmark = pytest.mark.security_regression

_SVC = "svcA"
_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver"}

_ENVELOPE = {
    "ok": True,
    "_debug_queries": [{"sql": "SELECT secret FROM t"}],
    "_debug_sqlite": [{"sql": "SELECT 1"}],
    "_debug_postgres": [{"sql": "SELECT 2"}],
    "_debug_calls": [{"path": "/service/abc"}],
    "_is_cached": False,
}


def _json_stream(payload: dict) -> StreamingResponse:
    body = json.dumps(payload).encode()

    async def _gen():
        yield body

    resp = StreamingResponse(_gen(), media_type="application/json")
    resp.headers["content-type"] = "application/json"
    return resp


def _strip(payload: dict, *, query: str = "disabled", api: str = "disabled") -> dict:
    settings = {"query_debug_visibility": query, "api_call_debug_visibility": api}
    with patch("backend.core.share_db.settings.get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
        out = asyncio.run(_strip_analyst_envelope(_json_stream(payload)))
    return json.loads(out.body)


def test_strip_removes_every_debug_key_when_analyst_visibility_is_disabled():
    assert _strip(_ENVELOPE) == {"ok": True}


def test_strip_removes_the_postgres_spelling_of_the_query_envelope():
    out = _strip({**_ENVELOPE, "debug_postgres": [{"sql": "SELECT 3"}]})

    assert not [k for k in out if "postgres" in k]


def test_strip_keeps_the_postgres_envelope_only_when_query_visibility_is_enabled_for_analysts():
    out = _strip(_ENVELOPE, query="analysts")

    assert out["_debug_postgres"] == [{"sql": "SELECT 2"}]
    assert out["_debug_sqlite"] == [{"sql": "SELECT 1"}]
    assert "_debug_calls" not in out


def _admin_app() -> FastAPI:
    from backend.core.request_context import build_request_context
    from backend.routers import admin, provision, services, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(provision.router)
    app.include_router(services.router)
    app.include_router(admin.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    return app


@pytest.fixture(autouse=True)
def _reset_debug_visibility_settings():
    yield
    share_db.set_setting("query_debug_visibility", "disabled")
    share_db.set_setting("api_call_debug_visibility", "disabled")


@pytest.fixture
def analyst_client():
    with TestClient(_admin_app()) as c:
        _start_share()
        _login_analyst(c, _seed_invite(service_ids=[_SVC]))
        yield c


_ADMIN_SITES = [
    ("POST", "/api/provision/validate", {"token": "t", "service_id": _SVC}),
    ("GET", "/api/provision/check-domain?prefix=abc", None),
    ("POST", "/api/provision/check-fos", {"bucket": "b", "region": "us-east-1", "access_key": "a", "secret_key": "s"}),
    ("GET", "/api/audit-logs", None),
    ("GET", "/api/admin/iceberg-calendar", None),
    ("GET", f"/api/services/{_SVC}/log-fields", None),
    ("POST", f"/api/services/{_SVC}/generate-viewer-key", None),
]


@pytest.mark.parametrize("visibility", ["disabled", "both"])
@pytest.mark.parametrize(("method", "path", "body"), _ADMIN_SITES)
def test_analyst_cannot_reach_envelope_bearing_admin_routes(analyst_client, method, path, body, visibility):
    share_db.set_setting("query_debug_visibility", visibility)
    share_db.set_setting("api_call_debug_visibility", visibility)

    r = analyst_client.request(method, path, json=body, headers=_HEADERS)

    assert r.status_code == 403
    assert not [k for k in r.json() if k.startswith(("_debug", "debug_"))]
