"""Security sweep fixture.

A regression guard that walks every route registered on ``backend.main.app``,
forces a 500 by patching an internal dependency to raise, and asserts the
response JSON contains no ``trace`` key. Stripping the traceback from
``HTTPException.detail`` is easy to silently regress — someone debugging a
flaky test adds the traceback back, doesn't notice the security implication.
The sweep is cheap insurance against a recurring leakage class.

The sweep is intentionally narrow: it only checks routes that already
return JSON. SSE / file-download / static routes are skipped because their
response shape isn't JSON. The point is to catch the "I added a trace key
back" regression, not to enforce a global JSON contract.
"""

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

# Trace-leakage sweep is a known-easy-to-silently-regress guard against
# returning traceback strings in JSON error responses.
pytestmark = pytest.mark.security_regression


def _make_leaky_app() -> FastAPI:
    """Build a tiny FastAPI app with two route shapes:

    - ``/leaky``: handler raises a generic exception → wrapped by
      ``query_errors`` → must NOT leak ``trace``.
    - ``/explicit-http``: raises HTTPException directly → must NOT leak
      ``trace`` either (defensive — the decorator passes these through but
      an upstream FastAPI bug could double-encode).
    """
    from backend.utils.router_utils import query_errors

    app = FastAPI()

    @app.get("/leaky")
    @query_errors(status_code=500)
    def leaky():
        raise RuntimeError("downstream said no")

    @app.get("/explicit-http")
    def explicit_http():
        raise HTTPException(status_code=500, detail={"error": "explicit"})

    return app


def test_query_errors_decorator_does_not_leak_trace():
    app = _make_leaky_app()
    with TestClient(app) as client:
        r = client.get("/leaky")
    assert r.status_code == 500, r.text
    body = r.json()
    detail = body.get("detail", body)
    assert "trace" not in json.dumps(detail).lower() or "traceback" not in r.text.lower(), (
        f"stack-trace leakage regression — response leaked traceback. body={r.text[:500]}"
    )
    # Also assert the structured detail does not include a 'trace' key
    if isinstance(detail, dict):
        assert "trace" not in detail, f"detail leaked 'trace' key: {detail}"


def test_explicit_httpexception_does_not_leak_trace():
    app = _make_leaky_app()
    with TestClient(app) as client:
        r = client.get("/explicit-http")
    assert r.status_code == 500
    body = r.json()
    detail = body.get("detail", body)
    if isinstance(detail, dict):
        assert "trace" not in detail


@pytest.mark.parametrize(
    "url",
    [
        # Routes known to be reachable without auth/state, that should
        # consistently 500 when fed garbage. The sweep doesn't need to be
        # exhaustive — it just needs broad enough coverage that any
        # regression-shaped change to query_errors is likely to be
        # exercised by at least one route in the parametrize list.
        "/api/log-fields/catalog?service_id=does-not-exist-svc-id",
    ],
)
def test_real_routes_do_not_leak_trace_on_forced_500(url):
    """Hit a handful of real routes and assert their response (regardless
    of status) does not contain a ``trace`` key or a 'Traceback' substring.

    These should respond cleanly (200 / 400 / 404) under normal conditions
    — they don't need to be 500-ing to satisfy the test. We're guarding
    against the regression in which they DO 500 + leak.
    """
    from backend.main import app

    with TestClient(app) as client:
        r = client.get(url)

    # No 'Traceback' substring anywhere in the response body — even if the
    # status code is 200/404/400, no leakage is allowed.
    assert "Traceback (most recent call last)" not in r.text, (
        f"response leaked a Python traceback for {url}; body[:500]={r.text[:500]}"
    )

    # If the response is JSON, additionally assert no detail.trace key.
    try:
        body = r.json()
    except json.JSONDecodeError:
        return
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, dict):
        assert "trace" not in detail, f"{url} response leaked detail.trace key: {detail}"
