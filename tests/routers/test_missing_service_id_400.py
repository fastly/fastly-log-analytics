"""A request without a service id answers 400 with the error envelope, never 422.

Runs against the real app with no dependency overrides. ``isolate_metadata_db``
points ``CONFIGS_DIR`` at an empty sandbox, so there is no active service to
fall back to and every route below has to reject the request itself.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from backend.main import app

# Routes that require an explicit service id and report it as ``service_id_required``.
EXPLICIT_SERVICE_ID_ROUTES = [
    ("GET", "/api/admin/slow-queries", None),
    ("GET", "/api/admin/slow-queries/count", None),
    ("DELETE", "/api/admin/slow-queries", None),
    ("GET", "/api/admin/clickhouse/status", None),
    ("GET", "/api/admin/vcl-health", None),
    ("GET", "/api/admin/ledger/quarantine", None),
    ("GET", "/api/provision/check-config", {"token": "t", "cdn_service_id": "c", "bucket": "b"}),
    ("GET", "/api/provision/ngwaf-workspaces", {"token": "t"}),
    ("GET", "/api/log-extents/stream", None),
    ("DELETE", "/api/alerts/alert-1", None),
    ("DELETE", "/api/views/view-1", None),
]

# Analytics routes resolve the service through RequestContext and report ``no_service``.
ANALYTICS_ROUTES = [
    ("POST", "/api/dashboard/aggregates"),
    ("POST", "/api/origin/aggregates"),
    ("POST", "/api/security/aggregates"),
    ("POST", "/api/performance/aggregates"),
    ("POST", "/api/network-health"),
]


@pytest.fixture
def bare_client():
    saved = dict(app.dependency_overrides)
    app.dependency_overrides.clear()
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(saved)


@pytest.mark.parametrize(("method", "path", "params"), EXPLICIT_SERVICE_ID_ROUTES)
def test_missing_service_id_is_400_service_id_required(bare_client, method, path, params) -> None:
    resp = bare_client.request(method, path, params=params)

    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"] == {"error": "service_id_required"}


@pytest.mark.parametrize(("method", "path"), ANALYTICS_ROUTES)
def test_analytics_route_without_service_is_400_envelope(bare_client, method, path) -> None:
    resp = bare_client.request(method, path, json={})

    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert detail["error"] == "no_service"
    assert detail["no_service"] is True
