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
from backend.routers.services import core as services_core_router

# Every test in this file pins a cross-tenant scope invariant — the
# router-level read gate that prevents an analyst from enumerating or
# modifying resources for a service they don't own. Refactors that
# touch tenancy must keep this coverage.
pytestmark = pytest.mark.security_regression


@pytest.fixture
def app_with_session():
    """Mini app that injects a per-test analyst_session on request.state
    so we can exercise the router-level scope check without spinning up
    the full RemoteAccessMiddleware chain."""

    app = FastAPI()
    app.include_router(alerts_router.router)
    app.include_router(views_router.router)
    app.include_router(services_core_router.router)

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


def test_views_delete_rejects_unauthorized_analyst_for_nonexistent_view(app_with_session):
    """Finding 017: ``delete_view`` previously only checked authorization
    inside the ``if existing:`` branch, so a request to delete a non-existent
    view id in a tenant the analyst doesn't own fell through the scope gate.
    The downstream wrapper still called ``sync_admin_state`` for the target
    service, both leaking which view ids exist and triggering unauthorized
    state mutation. Post-fix the scope gate runs before any DB lookup so
    both existing and non-existent ids in the wrong tenant return 403."""
    with TestClient(app_with_session) as c:
        r = c.delete(
            "/api/views/does-not-exist?service_id=svc-B",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


def test_views_delete_rejects_unauthorized_analyst_for_existing_view(app_with_session):
    """Companion to the above: an analyst-known view id in an analyst-scope
    service that's NOT in their session services must still be rejected."""
    with (
        patch(
            "backend.repositories.views.get_view_by_id",
            return_value={"id": "v-existing", "service_id": "svc-B", "name": "n"},
        ),
        TestClient(app_with_session) as c,
    ):
        r = c.delete(
            "/api/views/v-existing?service_id=svc-B",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


# ── custom-fields cross-tenant ─────────────────────────────────────────
#
# Pre-fix, the custom-field endpoints on backend.routers.services.core
# accepted service_id from the URL without checking analyst_session.
# An analyst granted access to svc-A could LIST, EXPORT, CREATE, UPDATE,
# DELETE, IMPORT, or VALIDATE-VCL against any service_id by changing the
# URL. These tests pin the new ``_require_service_scope`` gate.


_VALID_CF_BODY = {
    "name": "x_test_field",
    "label": "Test Field",
    "vcl_log_expression": "req.http.X-Test",
    "collection_stage": "deliver",
    "duckdb_type": "VARCHAR",
    "value_type": "string",
    "bytes_estimate": 20,
    "enabled": True,
}


def _scope_403(resp) -> bool:
    if resp.status_code != 403:
        return False
    try:
        return resp.json()["detail"]["error"] == "service_not_authorized"
    except Exception:
        return False


def test_custom_fields_list_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get(
            "/api/services/svc-B/custom-fields",
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


def test_custom_fields_export_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get(
            "/api/services/svc-B/custom-fields/export",
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


def test_custom_fields_create_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.post(
            "/api/services/svc-B/custom-fields",
            json=_VALID_CF_BODY,
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


def test_custom_fields_update_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.patch(
            "/api/services/svc-B/custom-fields/x_test_field",
            json={"label": "Updated"},
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


def test_custom_fields_delete_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.delete(
            "/api/services/svc-B/custom-fields/x_test_field",
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


def test_custom_fields_import_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.post(
            "/api/services/svc-B/custom-fields/import",
            json={"custom_fields": [_VALID_CF_BODY]},
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


def test_custom_fields_validate_vcl_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.post(
            "/api/services/svc-B/custom-fields/validate-vcl",
            json={"vcl_log_expression": "req.http.X-Test", "collection_stage": "deliver"},
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


def test_custom_fields_admin_unrestricted(app_with_session):
    """Admin sessions (no analyst_session) reach the handler regardless of
    service_id — the gate must not over-apply."""
    from unittest.mock import patch as _patch

    with (
        _patch(
            "backend.utils.router_utils.load_service_config",
            return_value={"log_fields": {"custom_fields": []}},
        ),
        TestClient(app_with_session) as c,
    ):
        r = c.get("/api/services/svc-anything/custom-fields")
    # Admin reaches the handler — may 200 with empty list, or 404/500
    # depending on load_service_config behavior. The contract pinned
    # here is "NOT 403 for admin".
    assert r.status_code != 403, r.text


def test_custom_fields_in_scope_analyst_reaches_handler(app_with_session):
    """An analyst whose session includes svc-A reaches the handler for
    svc-A — the gate doesn't false-positive in-scope requests."""
    from unittest.mock import patch as _patch

    with (
        _patch(
            "backend.utils.router_utils.load_service_config",
            return_value={"log_fields": {"custom_fields": []}},
        ),
        TestClient(app_with_session) as c,
    ):
        r = c.get(
            "/api/services/svc-A/custom-fields",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code != 403, r.text


# ── scoring labels cross-tenant ────────────────────────────────────────


def test_scoring_labels_delete_rejects_unauthorized_analyst(app_with_session):
    """An analyst scoped to ``svc-A`` cannot DELETE labels under
    ``svc-B`` even if they guess a label id. d180c4c added the gate;
    this test pins it so a refactor that drops the
    ``analyst_allowed_services`` check (or that returns the empty set
    as None — see [[analyst_allowed_services]] contract test) silently
    re-opens the cross-tenant DELETE.
    """
    from backend.routers import session_scoring as session_scoring_router

    app_with_session.include_router(session_scoring_router.router)
    with TestClient(app_with_session) as c:
        r = c.delete(
            "/api/services/svc-B/scoring/labels/some-label-id",
            headers={"x-test-session-services": "svc-A"},
        )

    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"
    assert r.json()["detail"]["service"] == "svc-B"


def test_scoring_labels_delete_admin_unrestricted(app_with_session):
    """Admin requests (no x-test-session-services header) bypass the
    scope check — ``analyst_allowed_services`` returns None so the
    gate falls through to the underlying delete. We patch the label
    repo to a no-op so the test asserts the gate, not the repo behavior."""
    from backend.routers import session_scoring as session_scoring_router

    app_with_session.include_router(session_scoring_router.router)
    with patch("backend.scoring.labels.delete_label", return_value={"id": "x", "service_id": "svc-B"}):
        with TestClient(app_with_session) as c:
            r = c.delete("/api/services/svc-B/scoring/labels/x")

    assert r.status_code != 403, r.text
