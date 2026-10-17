"""Analyst responses from the RUM routes must not carry ``_debug_*`` telemetry unless the share setting allows it.

``routers/rum._inject_telemetry`` injects ``_debug_queries`` / ``_debug_sqlite`` / ``_debug_calls`` /
``_is_cached`` UNCONDITIONALLY (it ignores ``DEBUG_RESPONSES``); the only thing keeping SQL text and
FOS/CDN call details away from an analyst is ``RemoteAccessMiddleware._strip_analyst_envelope``. These
tests drive the REAL RUM router behind the REAL middleware so that guard cannot silently regress.
"""

from __future__ import annotations

from unittest.mock import PropertyMock, patch

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
    with (
        patch("backend.routers.rum.rum_source_for", return_value={"name": f"{_SVC}_rum"}),
        patch("backend.routers.rum._ConnectionHolder", _Holder),
        patch("backend.routers.rum.execute_with_stale_view_retry", return_value={"no_data": True}),
    ):
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


class _Holder:
    def __init__(self, *_a, **_k):
        pass

    def __enter__(self):
        return object()

    def __exit__(self, *_a):
        return False


def _analytics_success(client):
    """The data-bearing branch builds its own dict and calls ``_inject_telemetry`` separately from no_data."""
    db_res = {
        "no_data": False,
        "total_beacons": 7,
        "pageviews": 5,
        "interactions": 1,
        "errors_count": 1,
        "vitals_rows": [("LCP", 2100.0, 10, 8, 1, 1)],
        "worst_pages_rows": [("/p", 4, 120.0, 2100.0, 0.05, 0.1)],
        "errors_rows": [("boom", "app.js", 3, 4, 2)],
        "trends_rows": [],
        "browsers": {"Firefox": 7},
        "os": {"macOS": 7},
        "devices": {"desktop": 7},
    }
    with (
        patch("backend.routers.rum.rum_source_for", return_value={"name": f"{_SVC}_rum"}),
        patch("backend.routers.rum._ConnectionHolder", _Holder),
        patch("backend.routers.rum.execute_with_stale_view_retry", return_value=db_res),
    ):
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


_HS_ANALYTICS = {
    "is_mock": False,
    "no_data": False,
    "beacon_count": 3,
    "pageview_count": 3,
    "interaction_count": 0,
    "error_count": 0,
    "vitals": {
        "lcp": {"p75": 2.1, "distribution": {"good": 80, "needs_improvement": 15, "poor": 5}},
        "cls": {"p75": 0.05, "distribution": {"good": 90, "needs_improvement": 8, "poor": 2}},
        "inp": {"p75": 180, "distribution": {"good": 85, "needs_improvement": 10, "poor": 5}},
        "fid": {"p75": 12.0, "fcp": 1.1, "ttfb": 0.4},
    },
    "worst_pages": [],
    "errors": [],
    "trends": {
        "timestamps": ["2026-10-17T00:00:00Z"],
        "lcp": [2.1],
        "cls": [0.05],
        "error_rate": [0.0],
        "pageviews": [3],
        "interactions": [0],
        "errors": [0],
    },
    "environments": {"browsers": {}, "os": {}, "devices": {}},
}


def _hs_beacon_health(client):
    """High-scale branch: real ``rum_beacon_health`` over a stub ClickHouse client."""

    class _Client:
        def execute(self, sql, params=None):
            return [{"c": 3}]

    class _Service:
        service_id = _SVC
        client = _Client()

    with patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs:
        hs.return_value = _Service()
        return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _hs_analytics(client):
    with (
        patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs,
        patch("backend.high_scale.rum.rum_analytics", return_value=dict(_HS_ANALYTICS)),
    ):
        hs.return_value = object()
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


_ROUTES = [
    _beacon_health_disabled,
    _beacon_health_check_failed,
    _analytics_no_data,
    _analytics_success,
    _hs_beacon_health,
    _hs_analytics,
]


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


def test_analyst_high_scale_query_response_has_no_debug_envelope() -> None:
    """``backend/high_scale/query.py`` returns BOTH ``debug_queries`` and ``_debug_queries`` (plus
    ``debug_calls``) at the top level; the analyst middleware must strip every spelling."""
    from backend.core.request_context import build_request_context
    from backend.routers import query, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(query.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    hs_result = {
        "columns": ["n"],
        "data": [{"n": 1}],
        "row_count": 1,
        "total_rows": 1,
        "truncated": False,
        "elapsed_ms": 1,
        "debug_queries": [{"sql": "SELECT secret FROM ch_table", "time_ms": 1, "engine": "ClickHouse"}],
        "_debug_queries": [{"sql": "SELECT secret FROM ch_table", "time_ms": 1, "engine": "ClickHouse"}],
        "debug_calls": [{"method": "GET", "path": "/internal"}],
    }

    def _post(c):
        with (
            patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs,
            patch("backend.high_scale.query.query_endpoint", return_value=dict(hs_result)),
        ):
            hs.return_value = object()
            return c.post(
                f"/api/query?service_id={_SVC}",
                json={"sql": "SELECT 1"},
                headers={**_HEADERS, "Origin": "https://testserver"},
            )

    with TestClient(app) as c:
        _start_share()
        _login_analyst(c, _seed_invite(service_ids=[_SVC]))
        resp = _post(c)
        assert resp.status_code == 200, resp.text
        assert _envelope_keys(resp.json()) == []
        assert "ch_table" not in resp.text

        share_db.set_setting("query_debug_visibility", "analysts")
        body = _post(c).json()
        assert "_debug_queries" in body
        assert "debug_calls" not in body and "_debug_calls" not in body
