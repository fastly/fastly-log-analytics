"""Analyst responses from the RUM routes must not carry ``_debug_*`` telemetry unless the share setting allows it.

``routers/rum._inject_telemetry`` injects ``_debug_queries`` / ``_debug_sqlite`` / ``_debug_calls`` /
``_is_cached`` UNCONDITIONALLY (it ignores ``DEBUG_RESPONSES``); the only thing keeping SQL text and
FOS/CDN call details away from an analyst is ``RemoteAccessMiddleware._strip_analyst_envelope``. These
tests drive the REAL RUM router behind the REAL middleware so that guard cannot silently regress.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.utils.remote_access import RemoteAccessMiddleware
from tests.conftest import override_request_context
from tests.remote_access.test_middleware import _login_analyst, _seed_invite, _start_share

pytestmark = pytest.mark.security_regression

_SVC = "svcA"
_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver"}
_ENVELOPE_PREFIXES = ("_debug", "debug_")


def _rum_app() -> FastAPI:
    from backend.core.request_context import build_request_context
    from backend.routers import rum, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(rum.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    return app


@pytest.fixture
def analyst_client():
    with TestClient(_rum_app()) as c:
        _start_share()
        _login_analyst(c, _seed_invite(service_ids=[_SVC]))
        yield c


def _beacon_health_disabled(client):
    return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _beacon_health_check_failed(client):
    cfg = {"rum": {"enabled": True}}
    with (
        patch("backend.routers.rum.svcconfig.load_config", return_value=cfg),
        patch("backend.routers.rum.rum_source_for", side_effect=RuntimeError("boom")),
    ):
        return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _analytics_no_data(client):
    class _Holder:
        def __init__(self, *_a, **_k):
            pass

        def __enter__(self):
            return object()

        def __exit__(self, *_a):
            return False

    with (
        patch("backend.routers.rum.rum_source_for", return_value={"name": f"{_SVC}_rum"}),
        patch("backend.routers.rum._ConnectionHolder", _Holder),
        patch("backend.routers.rum.execute_with_stale_view_retry", return_value={"no_data": True}),
    ):
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


_ROUTES = [_beacon_health_disabled, _beacon_health_check_failed, _analytics_no_data]


def _envelope_keys(body: dict) -> list[str]:
    return [k for k in body if k.startswith(_ENVELOPE_PREFIXES) or k == "_is_cached"]


@pytest.mark.parametrize("call", _ROUTES, ids=lambda f: f.__name__)
def test_analyst_rum_response_has_no_debug_envelope_by_default(analyst_client, call) -> None:
    resp = call(analyst_client)
    assert resp.status_code == 200, resp.text
    assert _envelope_keys(resp.json()) == []


@pytest.mark.parametrize("call", _ROUTES, ids=lambda f: f.__name__)
def test_share_setting_opens_only_the_matching_envelope_keys(analyst_client, call) -> None:
    """The injection is real (otherwise the default-case test proves nothing): the share settings are
    what release the keys, and each setting releases only its own half."""
    share_db.set_setting("query_debug_visibility", "analysts")
    body = call(analyst_client).json()
    assert "_debug_queries" in body
    assert "_debug_calls" not in body

    share_db.set_setting("query_debug_visibility", "disabled")
    share_db.set_setting("api_call_debug_visibility", "both")
    body = call(analyst_client).json()
    assert "_debug_calls" in body
    assert "_debug_queries" not in body
    assert "_debug_sqlite" not in body


def test_admin_rum_response_still_carries_the_envelope() -> None:
    """Control: the loopback admin (no analyst session) keeps the unconditional injection."""
    with TestClient(_rum_app()) as c:
        body = c.get(f"/api/services/{_SVC}/rum/beacon-health").json()
    assert {"_debug_queries", "_debug_calls"} <= set(body)
