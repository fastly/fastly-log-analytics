"""Verify that every registered endpoint returns 405 (not 404) for wrong HTTP methods.

The bug class this catches: a frontend caller uses the wrong HTTP method for a URL that
does exist. FastAPI returns 405 when a path matches but the method is wrong, and 404
when the path doesn't match anything. Getting 404 on a valid-looking path means the
client URL doesn't match the registered route — a URL construction or method mismatch bug.

A real example this would have caught: POST /api/services/{id}/ngwaf-sync was called
with GET (useSSE.start with no body defaults to GET), returning 404.
"""

import re

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.routing import Mount

from backend.main import app

# Substitute path parameters with benign test values so the path matches the route pattern.
_PATH_SUBS = {
    "{service_id}": "test-service-id",
    "{run_id}": "1",
    "{log_id}": "1",
    "{source_id}": "wellknown",
    "{alert_id}": "test-alert-id",
    "{view_id}": "test-view-id",
}

_METHOD_PRIORITY = ["GET", "POST", "PUT", "PATCH", "DELETE"]


def _sub_path(path: str) -> str:
    for placeholder, value in _PATH_SUBS.items():
        path = path.replace(placeholder, value)
    return path


def _pick_wrong_method(registered: set[str]) -> str | None:
    for m in _METHOD_PRIORITY:
        if m not in registered:
            return m
    return None  # route accepts every method — nothing to test


def _normalize_template(path: str) -> str:
    """Replace all {param_name} placeholders with {p} so structurally identical
    path patterns like /views/{service_id} and /views/{view_id} hash together."""
    return re.sub(r"\{[^}]+\}", "{p}", path)


def _norm_to_regex(norm: str) -> re.Pattern[str]:
    """Convert a normalised template to a regex that matches concrete paths."""
    parts = norm.split("{p}")
    return re.compile(
        "".join(re.escape(p) + "([^/]+)" if i < len(parts) - 1 else re.escape(p) for i, p in enumerate(parts))
    )


def _shadowed_by_another_route(path: str, wrong_method: str, own_norm: str) -> bool:
    """Return True if *path* would be matched AND handled by a different route that
    accepts *wrong_method*.  In that case, calling wrong_method on path would NOT
    return 405 — it would hit the other handler instead — so this case is untestable.

    Example: /api/alerts/preview is the literal path for POST /alerts/preview, but
    GET /alerts/{service_id} also matches "preview" as a service_id value.  Testing
    GET on /api/alerts/preview would invoke that handler (500 or 200), not return 405.
    """
    for norm, methods in _methods_by_norm.items():
        if norm == own_norm:
            continue
        if wrong_method not in methods:
            continue
        # Does this other normalised template match our concrete test path?
        if _norm_to_regex(norm).fullmatch(path):
            return True
    return False


# Build an app with the same routes but no static-file mount.
# The static mount at "/" intercepts GET requests on POST-only paths and returns
# 404 (file not found) instead of 405 (method not allowed), making the test useless.
_test_app = FastAPI()
for _r in app.routes:
    if not isinstance(_r, Mount):
        _test_app.routes.append(_r)

# Collect one test case per unique normalised path template, combining methods
# across all routes with the same structure.  Without normalisation, routes like
# /api/views/{service_id} (GET) and /api/views/{view_id} (DELETE) look like
# different templates but match the same URL.  Testing GET on /api/views/test-view-id
# would trigger the {service_id} GET handler (not a 405), masking the real issue.
#
# Canonical substituted path is taken from whichever route we see first.
_methods_by_norm: dict[str, set[str]] = {}
_path_by_norm: dict[str, str] = {}
for _route in app.routes:
    if isinstance(_route, APIRoute):
        _norm = _normalize_template(_route.path)
        _methods_by_norm.setdefault(_norm, set()).update(_route.methods or set())
        if _norm not in _path_by_norm:
            _path_by_norm[_norm] = _sub_path(_route.path)

_cases: list[tuple[str, str, str]] = []
for _norm, _all_methods in sorted(_methods_by_norm.items()):
    _path = _path_by_norm[_norm]
    _wrong = _pick_wrong_method(_all_methods)
    if _wrong and not _shadowed_by_another_route(_path, _wrong, _norm):
        _cases.append((_wrong, _path, str(sorted(_all_methods))))


@pytest.mark.parametrize(
    "wrong_method,path,registered",
    _cases,
    ids=[f"{wrong} {path}" for wrong, path, _ in _cases],
)
def test_wrong_http_method_returns_405_not_404(wrong_method: str, path: str, registered: str) -> None:
    """Calling the wrong HTTP method on a registered route must return 405, not 404.

    405 = route exists, method wrong.
    404 = route not found — the URL the client is calling doesn't match any registered path.
    """
    client = TestClient(_test_app, raise_server_exceptions=False)
    kwargs = {"json": {}} if wrong_method in ("POST", "PUT", "PATCH") else {}
    resp = getattr(client, wrong_method.lower())(path, **kwargs)
    assert resp.status_code == 405, (
        f"{wrong_method} {path} returned {resp.status_code} "
        f"(registered methods: {registered}). "
        f"A 404 here means the client URL doesn't match any route — check path construction."
    )
