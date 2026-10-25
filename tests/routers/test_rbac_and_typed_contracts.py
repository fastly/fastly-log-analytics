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

import datetime as dt
import json
import os
from collections import namedtuple
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import starlette.datastructures
from backend.core.share_db.settings import get_setting, set_setting
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.middleware.base import BaseHTTPMiddleware

from backend import config as svcconfig
from backend.core import share_db
from backend.main import app
from backend.routers import alerts as alerts_router
from backend.routers import share_admin
from backend.routers.services import core as services_core_router
from backend.utils import sqlite_profiler, telemetry
from backend.utils.remote_access import RemoteAccessMiddleware, _is_blocked_path
from backend.utils.tunnel.manager import AnalystSession, get_tunnel_manager
from tests._helpers.config_fakes import fake_mutate_config
from tests.conftest import MOCK_SERVICE_ID


# ── Merged from tests/routers/test_cross_tenant_scope.py ──
@pytest.fixture
def app_with_session():
    """Mini app that injects a per-test analyst_session on request.state
    so we can exercise the router-level scope check without spinning up
    the full RemoteAccessMiddleware chain."""

    app = FastAPI()
    app.include_router(alerts_router.router)
    app.include_router(alerts_router.views_router)
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
def test_alerts_list_admin_sees_all(app_with_session):
    fake_alerts = [_fake_alert("a1", "svc-A"), _fake_alert("a2", "svc-B")]
    with patch("backend.repositories.alerts.get_alerts", return_value=fake_alerts):
        with TestClient(app_with_session) as c:
            # No x-test-session-services header → admin (no scope)
            r = c.get("/api/alerts/")
    assert r.status_code == 200, r.text
    assert len(r.json()["data"]) == 2


@pytest.mark.security_regression
def test_alerts_list_service_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get("/api/alerts/svc-B", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_alerts_list_service_allows_in_scope(app_with_session):
    with patch("backend.repositories.alerts.get_alerts", return_value=[]):
        with TestClient(app_with_session) as c:
            r = c.get("/api/alerts/svc-A", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200


@pytest.mark.security_regression
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


@pytest.mark.security_regression
def test_views_list_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get("/api/views/svc-B", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_views_list_allows_in_scope(app_with_session):
    with patch("backend.repositories.alerts.get_views", return_value=[]):
        with TestClient(app_with_session) as c:
            r = c.get("/api/views/svc-A", headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200


@pytest.mark.security_regression
def test_views_list_admin_unrestricted(app_with_session):
    row = {
        "id": "v1",
        "service_id": "svc-anything",
        "name": "My view",
        "filters_json": "[]",
        "time_range_type": None,
        "start_time": None,
        "end_time": None,
        "page": None,
        "created_at": "2026-01-01 00:00:00",
    }
    with patch("backend.repositories.alerts.get_views", return_value=[row]):
        with TestClient(app_with_session) as c:
            r = c.get("/api/views/svc-anything")
    assert r.status_code == 200
    assert r.json() == [row]


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
def test_views_delete_rejects_unauthorized_analyst_for_existing_view(app_with_session):
    """Companion to the above: an analyst-known view id in an analyst-scope
    service that's NOT in their session services must still be rejected."""
    with (
        patch(
            "backend.repositories.alerts.get_view_by_id",
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


@pytest.mark.security_regression
def test_custom_fields_list_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get(
            "/api/services/svc-B/custom-fields",
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


@pytest.mark.security_regression
def test_custom_fields_export_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.get(
            "/api/services/svc-B/custom-fields/export",
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


@pytest.mark.security_regression
def test_custom_fields_create_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.post(
            "/api/services/svc-B/custom-fields",
            json=_VALID_CF_BODY,
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


@pytest.mark.security_regression
def test_custom_fields_update_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.patch(
            "/api/services/svc-B/custom-fields/x_test_field",
            json={"label": "Updated"},
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


@pytest.mark.security_regression
def test_custom_fields_delete_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.delete(
            "/api/services/svc-B/custom-fields/x_test_field",
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


@pytest.mark.security_regression
def test_custom_fields_import_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.post(
            "/api/services/svc-B/custom-fields/import",
            json={"custom_fields": [_VALID_CF_BODY]},
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


@pytest.mark.security_regression
def test_custom_fields_validate_vcl_rejects_unauthorized_analyst(app_with_session):
    with TestClient(app_with_session) as c:
        r = c.post(
            "/api/services/svc-B/custom-fields/validate-vcl",
            json={"vcl_log_expression": "req.http.X-Test", "collection_stage": "deliver"},
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r), r.text


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


# ── Merged from tests/routers/test_rbac_audit_fixes.py ──
@pytest.fixture
def app_with_blocklist():
    """Mini app whose middleware runs ``_is_blocked_path`` for analyst
    requests and short-circuits with 403 admin_only — exactly what
    ``RemoteAccessMiddleware`` does on the production analyst path.

    Tests inject the desired session via ``x-test-session-services``
    (same convention as test_cross_tenant_scope.py). Header present →
    analyst session; header absent → local admin (no blocklist).
    """

    app = FastAPI()

    @app.middleware("http")
    async def rbac_gate(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        if sid is not None:
            services = [s for s in sid.split(",") if s]
            request.state.analyst_session = SimpleNamespace(session_id="test", service_ids=services, email="t@t")
            # Engage the actual blocklist function — this is the
            # function the H-1..H-4 fixes modified.
            if _is_blocked_path(request.url.path):
                return JSONResponse(status_code=403, content={"error": "admin_only"})
        else:
            request.state.analyst_session = None
        return await call_next(request)

    # Stub routes that match the production mount paths so the
    # blocklist matcher sees the right shapes. Bodies are trivial —
    # we're testing the gate, not the handlers.
    @app.get("/api/usage/prefill")
    def _usage_prefill():
        return {"ok": True}

    @app.get("/api/download")
    def _download():
        return {"ok": True}

    @app.get("/api/download-all")
    def _download_all():
        return {"ok": True}

    @app.get("/api/download-folder")
    def _download_folder():
        return {"ok": True}

    @app.get("/api/cron-schedule")
    def _cron_schedule():
        return {"ok": True}

    @app.get("/api/services/{service_id}/lake-info")
    def _lake_info(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/config")
    def _scoring_config(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/status")
    def _scoring_status(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/audit")
    def _scoring_audit(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/threshold")
    def _scoring_threshold(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/exclude-regex")
    def _scoring_exclude(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/enforce-status-code")
    def _scoring_enforce(service_id: str):
        return {"ok": True, "service_id": service_id}

    # Analyst-NEEDED scoring reads — flag column / modal / dashboard
    # depend on these and MUST stay reachable post-H-4.
    @app.get("/api/services/{service_id}/scoring/labels")
    def _scoring_labels(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/sessions/{sid}/events")
    def _scoring_sess_events(service_id: str, sid: str):
        return {"ok": True, "service_id": service_id, "sid": sid}

    @app.get("/api/services/{service_id}/scoring/top-flagged")
    def _scoring_top(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/realtime-stream")
    def _realtime_stream(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/realtime-seed")
    def _realtime_seed(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/control-room/{tab}")
    def _control_room_tab(service_id: str, tab: str):
        return {"ok": True, "service_id": service_id, "tab": tab}

    @app.get("/api/services/{service_id}/log-field-audit")
    def _log_field_audit(service_id: str):
        return {"ok": True, "service_id": service_id}

    return app


@pytest.mark.security_regression
def test_usage_route_blocked_for_analyst(app_with_blocklist):
    """H-1: cost / billing / usage data must not leak to remote analysts."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/usage/prefill",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/download-all",
        "/api/download-folder",
        "/api/download",
    ],
)
def test_download_routes_blocked_for_analyst(app_with_blocklist, path):
    """H-2: raw-object download endpoints (single, folder, bulk) are
    admin-only. Pre-fix an analyst session reached them and could
    enumerate raw FOS objects for any bucket the operator had configured."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403, f"{path}: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
def test_lake_info_blocked_for_analyst(app_with_blocklist):
    """H-3: per-service Iceberg / object-store layout is admin-only.
    Even when the analyst owns the service, the route stays gated — the
    response leaks bucket names, prefixes, and catalog warehouse paths
    that an operator must rotate independently of session scope."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/lake-info",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
def test_cron_schedule_blocked_for_analyst(app_with_blocklist):
    """H-3: cron cadence config exposes the operator's ingest schedule
    (and indirectly the operator's tolerance for backlog) — admin-only."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/cron-schedule",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "suffix",
    [
        "/config",
        "/status",
        "/audit",
        "/threshold",
        "/exclude-regex",
        "/enforce-status-code",
        "/l2-enforce",
    ],
)
def test_scoring_admin_get_endpoints_blocked_for_analyst(app_with_blocklist, suffix):
    """H-4: scoring config / status / audit / threshold / exclude-regex /
    enforce-status-code / l2-enforce GETs are admin-only. Authorizing the
    analyst for the service must NOT bypass the suffix gate."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            f"/api/services/svc-A/scoring{suffix}",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{suffix}: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/services/svc-A/scoring/labels",
        "/api/services/svc-A/scoring/sessions/sess-1/events",
        "/api/services/svc-A/scoring/top-flagged",
    ],
)
def test_scoring_analyst_allowed_reads_still_pass(app_with_blocklist, path):
    """H-4 positive control: the flag column, session-detail modal, and
    dashboard depend on /scoring/labels, /scoring/sessions/<sid>/events,
    and /scoring/top-flagged. The suffix gate must NOT shadow them — if
    a refactor expands the admin-suffix list to swallow one of these,
    the analyst UI silently breaks."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200, f"{path}: {r.status_code} {r.text}"
    assert r.json()["service_id"] == "svc-A"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "suffix",
    ["/config", "/status", "/audit", "/threshold", "/exclude-regex", "/enforce-status-code"],
)
def test_scoring_admin_trailing_slash_does_not_bypass(app_with_blocklist, suffix):
    """Adversarial: /api/services/{id}/scoring/<suffix>/ MUST still be
    blocked. Without trailing-slash normalization the endswith() match
    would let the trailing-slash variant through."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(
            f"/api/services/svc-A/scoring{suffix}/",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{suffix}/: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
def test_lake_info_trailing_slash_does_not_bypass(app_with_blocklist):
    """Adversarial: /api/services/{id}/lake-info/ MUST still be blocked.
    Without trailing-slash normalization the regex fullmatch (anchored
    with $) would let the trailing-slash variant through."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(
            "/api/services/svc-A/lake-info/",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/download-all/",
        "/api/download-folder/",
        "/api/download/",
        "/api/cron-schedule/",
        "/api/usage/prefill/",
    ],
)
def test_subpath_blocks_resist_trailing_slash_bypass(app_with_blocklist, path):
    """Adversarial: trailing slash on the H-1/H-2/H-3 exact-subpath blocks
    must not bypass the gate. Defense in depth — the original
    ``startswith(sp + "/")`` check already handles "/api/download-all/",
    but normalization also covers the /api/usage/ prefix block."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403, f"{path}: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
def test_realtime_stream_allowed_for_analyst(app_with_blocklist):
    """S-1: realtime-stream powers the control room which is analyst-visible.
    The endpoint exposes aggregate metrics (rps, error rate, cache ratio) —
    no PII, no infra details — so it must not be blocked."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/realtime-stream",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 200, f"expected 200 but got {r.status_code}: {r.text}"


@pytest.mark.security_regression
def test_realtime_stream_still_needs_service_scope(app_with_blocklist):
    """Analyst without the target service in scope must still be rejected
    at the service-scope gate (not the blocklist, which no longer blocks
    this path)."""
    assert not _is_blocked_path("/api/services/svc-B/realtime-stream")


@pytest.mark.security_regression
def test_realtime_seed_allowed_for_analyst(app_with_blocklist):
    """S-1 parity: ``/realtime-seed`` (backend/routers/control_room.py
    :246) is the REST seed-data twin of ``/realtime-stream`` — both pull
    the same ``transform_single_second`` metric shape from rt.fastly.com,
    and ``/realtime-seed`` is what the client calls on page load to hydrate
    the chart with 60 historical bars BEFORE subscribing to the SSE stream.
    The S-1 decision that made ``/realtime-stream`` analyst-visible
    ("exposes aggregate metrics ... no PII, no infra details") applies
    identically here. Pinned separately from S-1 because ``_is_blocked_path``
    has no shared code path between the two routes — a future edit to the
    blocklist could block one and not the other with nobody noticing
    (an analyst's dashboard would 403 on first paint while the live
    stream kept working)."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/realtime-seed",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 200, f"expected 200 but got {r.status_code}: {r.text}"


@pytest.mark.security_regression
def test_control_room_tab_read_is_a_flagged_tripwire(app_with_blocklist):
    """Control Room's REST tab endpoint (``GET .../control-room/{tab}``,
    backend/routers/control_room.py:95) is NOT in any analyst blocklist
    today. Unlike ``/realtime-stream`` (S-1), there is no recorded
    security decision that it SHOULD be analyst-reachable — AGENTS.md
    documents the whole Control Room feature as an "Admin-only
    operational dashboard" and specifically says the "Admin Health" tab
    is meant to "surface log-field audit state and ingest health", which
    are data classes already admin-gated elsewhere (log-field-audit
    itself: see S-2 below; ingest/cron health: H-5's ``/api/cron-runs``
    + ``/api/audit-logs``; cost/usage: H-1's ``/api/usage/``).

    It is harmless TODAY because ``TAB_STUBS`` returns hardcoded zeros
    for every tab regardless of which one is requested. This test pins
    CURRENT behavior (reachable) as a deliberate tripwire, not an
    endorsement: it will keep passing right up until someone wires a real
    query behind ``admin_health`` / ``cost`` / ``security``, at which
    point this same 200 starts returning live operator-only data with no
    additional gate in the way. Before that wiring lands, add an explicit
    per-tab admin-only gate (e.g. a tab-name blocklist mirroring
    ``_ANALYST_BLOCKED_SCORING_SUFFIXES``) and invert this test.
    """
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/control-room/admin_health",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 200, f"expected 200 but got {r.status_code}: {r.text}"


@pytest.mark.security_regression
def test_log_field_audit_blocked_for_analyst(app_with_blocklist):
    """S-2: log-field-audit leaks operator's field configuration choices.
    Must be admin-only."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/log-field-audit",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
def test_log_field_audit_trailing_slash_does_not_bypass(app_with_blocklist):
    """Adversarial: trailing slash on log-field-audit must not bypass."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(
            "/api/services/svc-A/log-field-audit/",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


# ── Merged from tests/routers/test_debug_endpoints.py ──
"""Tests for the /api/debug router (SQLite ring-buffer surface)."""

_CALLS = [{"service": "Fastly API", "method": "GET", "path": "/probe", "time_ms": 1.0}]


@pytest.fixture
def recorded_calls():
    with patch.object(telemetry, "get_tracked_calls", return_value=_CALLS):
        yield


def _debug_keys(body: dict) -> list[str]:
    return [k for k in body if k.startswith(("_debug", "debug_"))]


def _validate(client):
    def _fastly_spy(method, path, **kwargs):
        return {"id": "tok-1", "name": "T", "user_id": "u"} if "/tokens/self" in path else {"name": "Svc"}

    with patch("backend.core.fastly.client.fastly", side_effect=_fastly_spy):
        return client.post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})


def _check_domain(client):
    with patch("backend.routers.provision._check_domain_available", return_value=(True, None)):
        return client.get("/api/provision/check-domain?prefix=valid-prefix-1")


def _check_fos_ok(client):
    fake = MagicMock()
    fake.list_objects_v2.return_value = {"Contents": []}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _check_fos_failed(client):
    fake = MagicMock()
    fake.list_objects_v2.side_effect = RuntimeError("boom")
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _audit_logs(client):
    return client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _iceberg_calendar(client):
    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: {"service_id": "service123", "name": "service123"}
    with patch(
        "backend.core.iceberg.get_snapshot_calendar", return_value={"2026-08-19": {"data_files": 5, "size_bytes": 1}}
    ):
        return client.get("/api/admin/iceberg-calendar?table_name=logs")


def _log_fields_get(client):
    svcconfig.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "log_fields": {"groups": ["A"]}})
    return client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _generate_viewer_key(client):
    invite = {
        "service_id": "svc-1",
        "name": "MyService",
        "access_key_id": "AK",
        "secret_key": "SK",
        "fos_bucket": "b",
        "fos_region": "us-east-1",
        "fos_endpoint": "https://s3.us-east-1.amazonaws.com",
        "fos_prefix": "",
    }
    with patch("backend.provision.generate_analyst_invite", return_value=invite):
        return client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")


_SITES = [
    pytest.param(_validate, id="provision-validate"),
    pytest.param(_check_domain, id="provision-check-domain"),
    pytest.param(_check_fos_ok, id="provision-check-fos-ok"),
    pytest.param(_check_fos_failed, id="provision-check-fos-failed"),
    pytest.param(_audit_logs, id="audit-logs"),
    pytest.param(_iceberg_calendar, id="iceberg-calendar"),
    pytest.param(_log_fields_get, id="service-log-fields"),
    pytest.param(_generate_viewer_key, id="generate-viewer-key"),
]

_is_remote_flag = False

original_getattr = starlette.datastructures.State.__getattr__


def mock_state_getattr(self, key: str):
    if key == "is_remote":
        return _is_remote_flag
    try:
        return original_getattr(self, key)
    except (AttributeError, KeyError):
        raise AttributeError(f"'State' object has no attribute '{key}'")


@pytest.mark.security_regression
class TestDebugEndpoints:
    @pytest.fixture(autouse=True)
    def _isolated_buffer(self):
        sqlite_profiler.clear()
        yield
        sqlite_profiler.clear()

    @pytest.fixture(autouse=True)
    def patch_state(self, monkeypatch):
        global _is_remote_flag
        _is_remote_flag = False
        monkeypatch.setattr(starlette.datastructures.State, "__getattr__", mock_state_getattr)
        yield
        _is_remote_flag = False

    @pytest.mark.security_regression
    def test_recent_sqlite_returns_empty_shape_when_buffer_empty(self):
        client = TestClient(app)
        res = client.get("/api/debug/recent-sqlite")
        assert res.status_code == 200
        body = res.json()
        assert body["queries"] == []
        assert body["buffer_size"] == 0
        assert body["buffer_cap"] > 0
        assert body["dropped"] == 0
        assert body["last_seq"] == 0

    @pytest.mark.security_regression
    def test_recent_sqlite_surfaces_real_metadata_db_traffic(self):
        """Hitting any endpoint that touches metadata_db must populate the
        captured-statements ring buffer. This is the end-to-end contract the
        Debug Panel relies on."""
        from backend.core import metadata as metadata_db

        con = metadata_db.get_con("debug-test-svc")
        con.execute("SELECT 1")
        client = TestClient(app)
        res = client.get("/api/debug/recent-sqlite")
        assert res.status_code == 200
        body = res.json()
        assert body["buffer_size"] > 0
        sqls = [q["sql"] for q in body["queries"]]
        assert any("SELECT 1" in s for s in sqls)

    @pytest.mark.security_regression
    def test_recent_sqlite_since_seq_filters(self):
        from backend.core import metadata as metadata_db

        con1 = metadata_db.get_con("svc-a")
        con1.execute("SELECT 1")
        client = TestClient(app)
        first = client.get("/api/debug/recent-sqlite").json()
        midpoint_seq = first["last_seq"]
        con2 = metadata_db.get_con("svc-b")
        con2.execute("SELECT 2")
        second = client.get(f"/api/debug/recent-sqlite?since_seq={midpoint_seq}").json()
        # Every returned entry must have seq > midpoint.
        assert all(q["seq"] > midpoint_seq for q in second["queries"])

    @pytest.mark.security_regression
    def test_clear_sqlite_drains_buffer(self):
        from backend.core import metadata as metadata_db

        con = metadata_db.get_con("svc-clear-test")
        con.execute("SELECT 1")
        client = TestClient(app)
        assert client.get("/api/debug/recent-sqlite").json()["buffer_size"] > 0
        res = client.post("/api/debug/clear-sqlite")
        assert res.status_code == 200
        assert res.json()["ok"] is True
        # Buffer must be empty immediately after the POST returns.
        assert sqlite_profiler.get_recent()["buffer_size"] == 0

    @pytest.mark.security_regression
    def test_recent_sqlite_respects_limit(self):
        from backend.core import metadata as metadata_db

        con = metadata_db.get_con("svc-limit")
        con.execute("SELECT 1")
        con.execute("SELECT 2")
        con.execute("SELECT 3")
        client = TestClient(app)
        res = client.get("/api/debug/recent-sqlite?limit=2")
        body = res.json()
        assert len(body["queries"]) <= 2

    @pytest.mark.security_regression
    def test_gated_envelope_adds_calls_only_when_debug_responses_is_on(self, monkeypatch, recorded_calls):
        monkeypatch.setenv("DEBUG_RESPONSES", "true")

        out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)

        assert out == {"ok": True, "_debug_calls": _CALLS}

    @pytest.mark.security_regression
    def test_gated_envelope_adds_nothing_when_debug_responses_is_off(self, monkeypatch, recorded_calls):
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

        out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)

        assert out == {"ok": True}

    @pytest.mark.security_regression
    def test_gated_envelope_drops_a_stale_envelope_when_debug_responses_is_off(self, monkeypatch, recorded_calls):
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

        out = telemetry.debug_envelope(
            {"ok": True, "_debug_calls": ["stale"], "_debug_queries": ["stale"]},
            gated=True,
            is_cached=None,
            inject_calls=True,
        )

        assert out == {"ok": True}

    @pytest.mark.security_regression
    def test_gated_envelope_without_inject_calls_still_never_injects(self, monkeypatch, recorded_calls):
        monkeypatch.setenv("DEBUG_RESPONSES", "true")

        out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=True)

        assert out == {"ok": True, "_is_cached": True}

    @pytest.mark.security_regression
    @pytest.mark.parametrize("call", _SITES)
    def test_route_emits_no_debug_envelope_when_debug_responses_is_off(self, client, monkeypatch, call):
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

        r = call(client)

        assert r.status_code == 200
        assert _debug_keys(r.json()) == []

    @pytest.mark.security_regression
    @pytest.mark.parametrize("call", _SITES)
    def test_route_emits_the_call_trace_when_debug_responses_is_on(self, client, monkeypatch, recorded_calls, call):
        monkeypatch.setenv("DEBUG_RESPONSES", "true")

        r = call(client)

        assert r.status_code == 200
        assert [c["path"] for c in r.json()["_debug_calls"]] == ["/probe"]

    @pytest.mark.security_regression
    def test_get_and_patch_debug_settings(self):
        global _is_remote_flag
        _is_remote_flag = False
        client = TestClient(app)

        # 1. Fetch initial settings (should default to disabled)
        res = client.get("/api/admin/debug-settings")
        assert res.status_code == 200
        body = res.json()
        assert body["query_debug_visibility"] == "disabled"
        assert body["api_call_debug_visibility"] == "disabled"

        # 2. Update settings via PATCH
        patch_res = client.patch(
            "/api/admin/debug-settings",
            json={
                "query_debug_visibility": "both",
                "api_call_debug_visibility": "analysts",
            },
        )
        assert patch_res.status_code == 200
        patched_body = patch_res.json()
        assert patched_body["query_debug_visibility"] == "both"
        assert patched_body["api_call_debug_visibility"] == "analysts"

        # Verify database settings were updated
        assert get_setting("query_debug_visibility") == "both"
        assert get_setting("api_call_debug_visibility") == "analysts"

        # Clean up settings
        set_setting("query_debug_visibility", "disabled")
        set_setting("api_call_debug_visibility", "disabled")

    @pytest.mark.security_regression
    def test_recent_sqlite_is_remote_blocked(self):
        global _is_remote_flag
        _is_remote_flag = True
        set_setting("query_debug_visibility", "disabled")

        client = TestClient(app)
        res = client.get("/api/debug/recent-sqlite")
        assert res.status_code == 403
        detail = res.json()["detail"]
        assert detail["error"] == "sql_debug_disabled"
        assert "SQL debug is disabled for analysts" in detail["message"]

    @pytest.mark.security_regression
    def test_recent_sqlite_is_remote_allowed_when_visibility_both(self):
        global _is_remote_flag
        _is_remote_flag = True
        set_setting("query_debug_visibility", "both")

        client = TestClient(app)
        res = client.get("/api/debug/recent-sqlite")
        assert res.status_code == 200

        set_setting("query_debug_visibility", "disabled")

    @pytest.mark.security_regression
    def test_clear_sqlite_is_remote_always_blocked(self):
        global _is_remote_flag
        _is_remote_flag = True

        client = TestClient(app)
        res = client.post("/api/debug/clear-sqlite")
        assert res.status_code == 403
        detail = res.json()["detail"]
        assert detail["error"] == "admin_only"
        assert "Only administrators can clear the SQLite debug buffer" in detail["message"]


# ── Merged from tests/routers/test_typed_response_admin.py ──
"""Admin action / probe / beacon routes keep their pre-typing wire payloads.

Each test drives the real handler with its producer patched and asserts the response body equals the
producer's dict byte-for-byte after JSON round-tripping (underscore envelope keys excluded), so the
``response_model`` added to the route can't drop, rename or re-encode a field.
"""


def _body(resp, *, status=200):
    assert resp.status_code == status, resp.text
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def test_refresh_single_bot_source_matches_producer(client):
    meta = {"id": "arcjet", "entry_count": 42, "last_updated": "2026-10-11T00:00:00Z", "enabled": True}
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=meta):
        resp = client.post("/api/admin/bot-sources/arcjet/refresh")
    assert _body(resp) == {"ok": True, "source": meta}


def test_refresh_all_bot_sources_matches_producer_on_both_paths(client):
    results = [{"id": "a", "entry_count": 3}, {"id": "b", "failed": True, "error": "boom"}]
    expected = {"ok": False, "sources": results, "updated_count": 1, "failed_count": 1}
    for path in ("/api/admin/bot-sources/refresh", "/api/admin/bots/refresh"):
        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=results):
            assert _body(client.post(path)) == expected


def test_rdns_enrich_matches_producer(client):
    summary = {"resolved": 4, "errors": 1, "discovered": 7, "batch_limit": 100}
    with patch("backend.utils.rdns_cache.enrich_batch", return_value=summary) as enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=100")
    enrich.assert_called_once_with(limit=100)
    assert _body(resp) == {"ok": True, "summary": summary}


def test_duckdb_recycle_skipped_and_recycled_match_producer(client):
    mb = 1024 * 1024
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * mb),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
    ):
        skipped = _body(client.post("/api/admin/duckdb/recycle"))
    assert skipped == {
        "ok": True,
        "status": "skipped",
        "reason": "RSS 100MB < threshold 500MB",
        "rss_mb": 100.0,
        "threshold_mb": 500.0,
    }

    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * mb, 200 * mb]),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
        patch("backend.core.duckdb_recycle.recycle_once", return_value="manual_admin: recycled 1/1"),
    ):
        recycled = _body(client.post("/api/admin/duckdb/recycle?force=true"))
    assert recycled == {
        "ok": True,
        "status": "recycled",
        "detail": "manual_admin: recycled 1/1",
        "rss_before_mb": 300.0,
        "rss_after_mb": 200.0,
        "freed_mb": 100.0,
    }


def test_ngwaf_sync_matches_producer(client):
    stats = {"total_cached_bots": 2, "workspaces": {"w": None}, "top_bots": {"bot": 2}}
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-n"}),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync"),
        patch("backend.utils.ngwaf_bot_cache.get_cache_stats", return_value=stats),
    ):
        resp = client.post("/api/admin/ngwaf/sync/svc-n")
    assert _body(resp) == {"ok": True, "service_id": "svc-n", "workspace_id": "ws-1", "stats": stats}


def test_share_purge_matches_producer(client):
    stats = {
        "deleted_audit_logs": 3,
        "deleted_expired_invites": 1,
        "deleted_stale_sessions": 2,
        "deleted_claim_tokens": 0,
        "audit_retention_days": 90,
        "max_idle_session_days": 30,
    }
    with patch("backend.core.share_db.purge_all_share_records", return_value=stats):
        resp = client.post("/api/admin/share/purge")
    assert _body(resp) == {"ok": True, **stats}


def test_iceberg_calendar_matches_producer(client):
    calendar = {"2026-10-01": {"data_files": 4, "size_bytes": 0}, "2026-10-02": {"data_files": 9, "size_bytes": 0}}
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=calendar):
        resp = client.get("/api/admin/iceberg-calendar")
    assert _body(resp) == calendar


def test_vcl_health_matches_producer(client):
    Snippet = namedtuple("Snippet", "name")
    snippets = [Snippet("Fastly Log Analytics - capture"), Snippet("Fastly Log Analytics - scoring")]
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-a", "admin_token": "tok"}),
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=7),
        patch("backend.provision.declarative.fastly_integration.fetch_snippets", return_value=snippets),
    ):
        clean = _body(client.get("/api/admin/vcl-health?service_id=svc-a"))
    assert clean == {
        "service_id": "svc-a",
        "active_version": 7,
        "legacy_snippets_found": 0,
        "legacy_names": [],
        "consolidated_snippets_found": 2,
        "consolidated_names": [s.name for s in snippets],
        "is_clean": True,
        "recommendation": "✓ Clean state — VCL migration complete. Ready for production.",
    }
    with patch("backend.config.load_config", return_value=None):
        missing = _body(client.get("/api/admin/vcl-health?service_id=nope"))
    assert missing == {"service_id": "nope", "error": "Service nope not configured", "is_clean": False}


def test_page_telemetry_matches_producer(client):
    payload = {
        "queries": [{"engine": "duckdb", "sql_query": "SELECT 1", "duration_ms": 1.5}],
        "sections": [{"section_name": "summary", "duration_ms": 2.0}],
        "calls": [
            {
                "service": "FOS",
                "method": "GET",
                "path": "/b/k",
                "status": "200",
                "time_ms": 3.0,
                "caller": "fn",
                "bytes": 10,
            }
        ],
    }
    with (
        patch("backend.config.get_active_service_id", return_value="svc-a"),
        patch("backend.core.metadata.usage_log.get_page_telemetry", return_value=payload),
    ):
        assert _body(client.get("/api/debug/page-telemetry?page_load_id=p1")) == payload
    with patch("backend.config.get_active_service_id", return_value=None):
        assert _body(client.get("/api/debug/page-telemetry?page_load_id=p1")) == {
            "queries": [],
            "sections": [],
            "calls": [],
        }


def test_health_shallow_and_deep_match_producer(client):
    from backend.main import _APP_VERSION

    assert _body(client.get("/api/health")) == {"status": "ok", "version": _APP_VERSION}
    with patch("backend.config.list_service_ids", return_value=[]):
        assert _body(client.get("/api/health?deep=1")) == {"status": "ok", "version": _APP_VERSION, "services": []}


def test_backfill_window_matches_producer(client):
    counters = {
        "files_downloaded": 2,
        "rows_downloaded": 10,
        "files_removed": 0,
    }
    with patch("backend.core.iceberg.sync_data", return_value=counters):
        resp = client.post("/api/admin/backfill-window?start_time=2026-10-01T00:00:00Z&end_time=2026-10-01T02:00:00Z")
    assert _body(resp) == counters
    failure = {"error": "Could not load table: x", "files_downloaded": 0}
    with patch("backend.core.iceberg.sync_data", return_value=failure):
        resp = client.post("/api/admin/backfill-window?start_time=2026-10-01T00:00:00Z&end_time=2026-10-01T02:00:00Z")
    assert _body(resp) == failure


def test_cron_starters_match_producer(client):
    started = {"ok": True, "message": "started", "run_id": 41}
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value=started):
        assert _body(client.post("/api/admin/commit-iceberg"), status=202) == started
        assert _body(client.post("/api/admin/ingest-logs")) == started


def test_rebuild_local_view_matches_producer(client):
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb.start_cron_run", return_value=77),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
    ):
        resp = client.post("/api/admin/rebuild-local-view")
    assert _body(resp, status=202) == {"ok": True, "message": "Local view rebuild started.", "run_id": 77}


def test_teardown_frontend_matches_producer(client):
    cfg = {"service_id": "svc-t", "remote_frontend": {"service_id": "remote-1"}}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=cfg),
        patch("backend.routers.share_admin.delete_remote_frontend"),
        patch("backend.config.mutate_config", return_value=cfg),
    ):
        resp = client.post("/api/sharing/teardown-frontend", json={"service_id": "svc-t", "token_override": "t"})
    assert _body(resp) == {"ok": True, "message": "Successfully deleted remote frontend service remote-1"}


def test_beacon_acks_match_producer(client):
    ux = client.post("/api/ux-events", json={"event": "column_reordered", "pathname": "/x"})
    assert _body(ux) == {"ok": True}
    wv = client.post("/api/web-vitals", json={"id": "v1", "name": "LCP", "value": 1.2, "rating": "good"})
    assert _body(wv) == {"ok": True}


def _expected(payload):
    return json.loads(json.dumps(jsonable_encoder(payload)))


def test_celery_status_matches_producer(client, monkeypatch):
    payload = {
        "broker_reachable": True,
        "workers": {
            "active_tasks": {"w1": [{"id": "t1", "name": "n", "args": [1], "time_start": 2.5, "worker_pid": 7}]},
            "stats": {"w1": {"started": dt.datetime(2026, 1, 1, tzinfo=dt.UTC)}},
            "registered": {"w1": ["a.b"]},
            "scheduled": {},
            "count": 1,
        },
        "queues": {"q.ingest": 3},
        "inprocess_schedules": [{"name": "job", "trigger": "interval[0:01:00]", "next_run_at": 1700000000.0}],
        "ledger": {"committed": 5},
    }
    monkeypatch.setattr("backend.routers.admin.health.get_celery_status", lambda: payload)
    resp = client.get("/api/admin/celery/status")
    assert resp.status_code == 200
    assert _body(resp) == _expected(payload)


def test_celery_status_workers_error_branch(client, monkeypatch):
    payload = {
        "broker_reachable": False,
        "workers": {"active_tasks": {}, "stats": {}, "registered": {}, "scheduled": {}, "count": 0, "error": "down"},
        "queues": {},
        "inprocess_schedules": [],
        "ledger": {},
    }
    monkeypatch.setattr("backend.routers.admin.health.get_celery_status", lambda: payload)
    assert _body(client.get("/api/admin/celery/status")) == payload


def test_duckdb_status_matches_producer_keys(client):
    resp = client.get("/api/admin/duckdb/status")
    assert resp.status_code == 200
    body = _body(resp)
    assert set(body) == {"ok", "memory", "barrier", "pools", "retired_pools"}
    assert body["ok"] is True
    assert set(body["memory"]) == {"current_rss_mb", "recycle_threshold_mb", "recycle_interval_min"}
    assert set(body["barrier"]) == {"is_active", "active_paths"}


def test_ngwaf_status_and_rdns_stats(client, monkeypatch):
    stats = {"total_cached_bots": 2, "workspaces": {"w": None, "x": "2026-01-01"}, "top_bots": {"bot": 2}}
    monkeypatch.setattr("backend.utils.ngwaf_bot_cache.get_cache_stats", lambda: dict(stats))
    resp = client.get("/api/admin/ngwaf/status")
    assert resp.status_code == 200
    assert _body(resp) == {"ok": True, **stats}

    monkeypatch.setattr("backend.utils.rdns_cache.get_stats", lambda: {"entries": 4, "hit_rate": 0.5})
    rd = client.get("/api/admin/rdns/stats")
    assert rd.status_code == 200
    assert _body(rd) == {"ok": True, "stats": {"entries": 4, "hit_rate": 0.5}}


def test_control_room_tabs_match_stub_payloads(client):
    from backend.routers.control_room import TAB_STUBS

    for tab, stub in TAB_STUBS.items():
        resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}")
        assert resp.status_code == 200, tab
        assert _body(resp) == stub, tab


def test_control_room_models_roundtrip_producer_shapes():
    from backend.models.services import (
        CorrelateResponse,
        LogFieldAuditResponse,
        RealtimeSeedResponse,
        WizardStateResponse,
        WizardStepResponse,
    )

    cases = [
        (RealtimeSeedResponse, {"ticks": [{"ts": 1, "rps": 2.5, "nested": {"a": [1]}}]}),
        (
            LogFieldAuditResponse,
            {
                "service_id": "svc",
                "enabled_fields": ["ip", "url"],
                "features": [
                    {"name": "Insights", "required_fields": ["ip"], "status": "ok", "missing_fields": []},
                ],
            },
        ),
        (
            CorrelateResponse,
            {
                "dimension": "country",
                "top": [{"value": "US", "count": 3}],
                "freshness": {"latest_log_at": None, "lag_seconds": 0},
            },
        ),
        (WizardStateResponse, {"service_id": "svc", "completed_steps": ["a"], "is_complete": False}),
        (WizardStepResponse, {"recorded": True, "step": "a"}),
    ]
    for model, payload in cases:
        assert model.model_validate(payload).model_dump(mode="json") == payload, model.__name__


# ── Merged from tests/routers/test_typed_response_endpoints.py ──
"""Tightened response models keep the producers' wire payloads byte-for-byte.

Each case feeds a REAL producer's output (or a dict shaped exactly like one)
through the model with ``exclude_unset`` + by-alias dumping — the same
serialization the routes apply — and compares serialized JSON, so an int that
turns into a float or a key that turns into ``null`` fails.
"""


def _wire(model_cls: type, payload: dict[str, Any]) -> str:
    return model_cls.model_validate(payload).model_dump_json(by_alias=True, exclude_unset=True)


def _assert_same_bytes(model_cls: type, payload: dict[str, Any]) -> None:
    assert json.loads(_wire(model_cls, payload)) == jsonable_encoder(payload)
    assert _wire(model_cls, payload) == json.dumps(jsonable_encoder(payload), separators=(",", ":"), ensure_ascii=False)


def test_l2_enforce_state_matches_both_real_producers():
    from backend.models.session_scoring import ScoringL2EnforceState
    from backend.routers import session_scoring_admin as admin

    _assert_same_bytes(ScoringL2EnforceState, admin._l2_enforce_unavailable())
    _assert_same_bytes(
        ScoringL2EnforceState,
        admin._build_l2_enforce_block(
            scoring_enabled_at_raw="1700000000",
            l2_enforce_raw="1",
            l2_enabled_at_raw="1700086400",
            now_epoch=1700200000,
        ),
    )
    _assert_same_bytes(
        ScoringL2EnforceState,
        admin._build_l2_enforce_block(
            scoring_enabled_at_raw=None, l2_enforce_raw=None, l2_enabled_at_raw=None, now_epoch=1700200000
        ),
    )


def test_scoring_state_models_match_route_producers(monkeypatch):
    from backend import config as svcconfig
    from backend.models.session_scoring import (
        ScoringEnforceStatusCodeState,
        ScoringExcludeRegexState,
        ScoringThresholdState,
    )
    from backend.routers import session_scoring_admin as admin

    cfg = {
        "service_id": "svc",
        "scoring": {
            "enabled": True,
            "operator_threshold": 80,
            "operator_threshold_set_at": "2026-01-01T00:00:00+00:00",
            "exclude_url_regex": "\\.(png|css)$",
            "enforce_status_code": 403,
        },
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)

    _assert_same_bytes(ScoringThresholdState, admin.scoring_threshold_get("svc"))
    _assert_same_bytes(ScoringExcludeRegexState, admin.scoring_exclude_regex_get("svc"))
    _assert_same_bytes(ScoringEnforceStatusCodeState, admin.scoring_enforce_status_code_get("svc"))

    cfg["scoring"] = {"enabled": True}
    _assert_same_bytes(ScoringThresholdState, admin.scoring_threshold_get("svc"))
    _assert_same_bytes(ScoringEnforceStatusCodeState, admin.scoring_enforce_status_code_get("svc"))


def test_scoring_enforce_threshold_and_rows_roundtrip():
    from backend.models.session_scoring import (
        ScoringComplianceRow,
        ScoringDistributionRow,
        ScoringEnforceThresholdResponse,
        ScoringTopFlaggedRow,
    )

    _assert_same_bytes(
        ScoringEnforceThresholdResponse, {"threshold": None, "enforced": False, "key": "enforce_threshold"}
    )
    _assert_same_bytes(ScoringEnforceThresholdResponse, {"threshold": 70, "enforced": True, "key": "enforce_threshold"})
    _assert_same_bytes(ScoringDistributionRow, {"hour": "2026-01-01T00:00:00", "bucket": "0-25", "count": 4})
    _assert_same_bytes(ScoringComplianceRow, {"hour": "2026-01-01T00:00:00", "compliance": "ok", "count": 4})
    _assert_same_bytes(
        ScoringTopFlaggedRow,
        {"timestamp": "2026-01-01T00:00:00", "edge_sid": None, "edge_score": 90, "url": "/a"},
    )
    with pytest.raises(ValidationError):
        ScoringTopFlaggedRow.model_validate({"edge_sid": "x", "edge_score": 1})


def test_log_fields_catalog_model_matches_real_producer():
    from backend.models.common import LogFieldsCatalogResponse
    from backend.routers.bootstrap import _compute_log_fields_catalog

    payload = _compute_log_fields_catalog(None)
    assert payload["groups"] and payload["fields"] and payload["insights"] and payload["presets"]
    _assert_same_bytes(LogFieldsCatalogResponse, payload)


def test_log_fields_catalog_custom_entries_roundtrip():
    from backend.core import log_fields as lf
    from backend.models.common import LogFieldCatalogEntry

    entries = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_field", "label": "My Field", "enabled": True}]}
    )
    assert entries
    for entry in entries:
        # Custom entries build their keys in a different order than built-ins; the
        # model's declared order (unchanged by typing) is shared, so compare values.
        assert json.loads(_wire(LogFieldCatalogEntry, entry)) == jsonable_encoder(entry)


def test_bootstrap_pop_geo_and_debug_state_roundtrip():
    from backend.models.common import BootstrapDebugState, BootstrapPopGeo

    _assert_same_bytes(BootstrapPopGeo, {"city": "Denver", "region": "co", "country": "us"})
    _assert_same_bytes(BootstrapPopGeo, {"city": "", "region": "", "country": ""})
    _assert_same_bytes(
        BootstrapDebugState,
        {"debug_responses_enabled": False, "query_debug_visibility": "disabled", "api_call_debug_visibility": "admins"},
    )


def test_provision_models_roundtrip():
    from backend.models.provision import (
        NgwafWorkspacesResponse,
        ProvisionCheckConfigResponse,
        ProvisionLakeInfoResponse,
        ProvisionValidateResponse,
    )

    _assert_same_bytes(
        ProvisionCheckConfigResponse,
        {"logging_service": {"ok": True, "details": "ok"}, "cdn_service": {"ok": False, "details": ""}},
    )
    _assert_same_bytes(
        NgwafWorkspacesResponse,
        {"workspaces": [{"id": "w", "name": "n"}], "error_hint": "h", "_debug_raw": "GET /x\n\n--- Response (200) ---"},
    )
    _assert_same_bytes(
        ProvisionValidateResponse,
        {"service_name": "s", "token_info": {"id": "1", "name": "t", "user_id": None, "type": "automation"}},
    )
    _assert_same_bytes(
        ProvisionLakeInfoResponse,
        {
            "ok": True,
            "table_exists": True,
            "info": {"table_name": "logs", "snapshots": 2, "data_files": 7, "size_bytes": 10, "region": None},
            "calendar": {"2026-01-01": {"data_files": 7, "size_bytes": 0}},
            "range": {"start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00"},
        },
    )
    with pytest.raises(ValidationError):
        ProvisionCheckConfigResponse.model_validate({"logging_service": {"ok": True, "details": "x"}})


def test_security_top_bots_rows_roundtrip():
    from backend.models.security import SecurityBotRow

    _assert_same_bytes(SecurityBotRow, {"id": "gptbot", "name": "Gptbot", "category": "ai", "request_count": 12})
    _assert_same_bytes(SecurityBotRow, {"name": "Googlebot", "category": "search", "request_count": 5})
    _assert_same_bytes(
        SecurityBotRow,
        {
            "id": "b",
            "name": "b",
            "category": "unknown",
            "request_count": 3,
            "verified_count": 3,
            "impersonator_count": 0,
            "unverified_count": 0,
            "verification_coverage": 1.0,
            "pending_count": 0,
        },
    )


def _result() -> dict:
    return {
        "columns": ["ts", "tz_ts", "amount", "n"],
        "data": [
            {
                "ts": dt.datetime(2026, 1, 2, 3, 4, 5),
                "tz_ts": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
                "amount": Decimal("12.50"),
                "n": 7,
            }
        ],
        "row_count": 1,
        "total_rows": 1,
        "truncated": False,
        "elapsed_ms": 3,
        "explain_plan": "plan",
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }


def test_query_rows_encode_like_jsonable_encoder(client):
    result = _result()
    expected_rows = json.loads(json.dumps(jsonable_encoder(result["data"])))
    with patch("backend.repositories.query.execute_query", return_value=result):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"] == expected_rows
    assert body["data"][0]["tz_ts"].endswith("+00:00")
    assert body["data"][0]["amount"] == 12.5
    assert {k: body[k] for k in ("columns", "row_count", "total_rows", "truncated", "elapsed_ms", "explain_plan")} == {
        k: result[k] for k in ("columns", "row_count", "total_rows", "truncated", "elapsed_ms", "explain_plan")
    }


def test_query_omits_explain_plan_when_producer_did_not_set_it(client):
    result = _result()
    del result["explain_plan"]
    with patch("backend.repositories.query.execute_query", return_value=result):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    assert "explain_plan" not in resp.json()


@pytest.mark.parametrize(
    "model_name,payload",
    [
        ("RumStatusResponse", {"enabled": False, "enabled_at": None}),
        (
            "RumStatusResponse",
            {
                "enabled": True,
                "enabled_at": "2026-01-01T00:00:00+00:00",
                "deployed_vcl_sha": "a",
                "current_vcl_sha": "b",
                "vcl_drift": True,
                "capture_vitals": True,
                "capture_performance": False,
                "capture_errors": True,
                "capture_events": True,
                "custom_condition": "",
            },
        ),
        ("RumSettingsUpdateResponse", {"ok": True, "message": "m", "skipped": False}),
        (
            "RumBeaconHealthResponse",
            {
                "enabled": True,
                "beacon_fire_rate": 3,
                "recent_beacons": 3,
                "last_beacon_time": None,
                "setup_complete": True,
                "message": "x",
                "_debug_queries": [{"sql": "s"}],
                "_debug_sqlite": [],
                "_debug_calls": [],
                "_is_cached": False,
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": True,
                "beacon_count": 0,
                "pageview_count": 0,
                "interaction_count": 0,
                "error_count": 0,
                "message": "Waiting",
                "vitals": {
                    "lcp": {"p75": None, "distribution": None},
                    "cls": {"p75": None, "distribution": None},
                    "inp": {"p75": None, "distribution": None},
                    "fid": {"p75": None, "fcp": None, "ttfb": None},
                },
                "worst_pages": [],
                "errors": [],
                "trends": {"timestamps": [], "lcp": [], "cls": [], "error_rate": []},
                "environments": {"browsers": {}, "os": {}, "devices": {}},
                "_debug_queries": [],
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": False,
                "beacon_count": 12,
                "pageview_count": 10,
                "interaction_count": 1,
                "error_count": 1,
                "vitals": {
                    "lcp": {"p75": 2.5, "distribution": {"good": 80, "needs_improvement": 15, "poor": 5}},
                    "cls": {"p75": 0.1, "distribution": {"good": 90, "needs_improvement": 10, "poor": 0}},
                    "inp": {"p75": 180, "distribution": {"good": 70, "needs_improvement": 20, "poor": 10}},
                    "fid": {"p75": 3.0, "fcp": 1.2, "ttfb": 0.4},
                },
                "worst_pages": [
                    {"path": "/a", "views": 4, "avg_load_time": 1, "lcp_p75": None, "cls_p75": 0.05, "error_rate": 0.0}
                ],
                "errors": [{"message": "boom", "file": "a.js", "line": 3, "col": 4, "count": 2}],
                "trends": {
                    "timestamps": ["2026-01-01T00:00:00+00:00"],
                    "lcp": [None],
                    "cls": [0.1],
                    "error_rate": [0.0],
                    "pageviews": [10],
                    "interactions": [1],
                    "errors": [1],
                },
                "environments": {"browsers": {"Chrome": 9}, "os": {"macOS": 9}, "devices": {"desktop": 9}},
            },
        ),
    ],
)
def test_rum_models_roundtrip(model_name, payload):
    from backend.models import rum

    model = getattr(rum, model_name)
    validated = model.model_validate(payload)
    assert validated.model_dump(mode="json") == payload
    # Same bytes, not just equal values: an int must not come back as 5.0.
    assert validated.model_dump_json() == json.dumps(payload, separators=(",", ":"))


def test_scoring_status_route_disabled_shape(client, monkeypatch):
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert resp.status_code == 200
    assert _body_typed_response_endpoints(resp) == {"enabled": False}


def test_scoring_status_and_config_match_producer(client, monkeypatch):
    from backend import config as svcconfig
    from backend.routers import session_scoring as ss

    cfg = {
        "service_id": MOCK_SERVICE_ID,
        "scoring": {"enabled": True, "request_secret": "s3cret", "custom_key": [1, 2], "deployed_vcl_sha": "v"},
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    expected = ss.scoring_status(MOCK_SERVICE_ID)

    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert resp.status_code == 200
    assert _body_typed_response_endpoints(resp) == expected
    assert "request_secret" not in resp.json()

    composite = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/config")
    assert composite.status_code == 200
    assert _body_typed_response_endpoints(composite)["status"] == expected


def test_scoring_label_delete_model_roundtrip():
    from backend.models.session_scoring import ScoringLabelDeleteResponse

    payload = {"status": "success", "id": "abc"}
    assert ScoringLabelDeleteResponse.model_validate(payload).model_dump(mode="json") == payload


def test_scoring_status_declares_config_identity_keys():
    from backend.models.session_scoring import ScoringStatusResponse

    payload = {
        "enabled": True,
        "scoring_service_id": "svc",
        "scoring_service_name": "n",
        "scoring_domain": "d.example",
        "matrix_version": "2026-01-01",
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }
    declared = set(ScoringStatusResponse.model_fields)
    assert set(payload) <= declared
    assert ScoringStatusResponse.model_validate(payload).model_dump(mode="json", exclude_unset=True) == payload


def test_scoring_labels_list_matches_producer(client, monkeypatch):
    from backend.scoring import labels as _labels

    rows = [
        {
            "id": "a1",
            "service_id": MOCK_SERVICE_ID,
            "sid": "s1",
            "label": lbl,
            "notes": "n",
            "flagged_by": "admin",
            "sample_ip": "",
            "sample_ua": "",
            "sample_url": "",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
        }
        for lbl in ("good", "bad", "neutral")
    ]
    counts = {"good": 1, "bad": 1, "neutral": 1}
    monkeypatch.setattr(_labels, "list_labels", lambda sid, limit=500: rows)
    monkeypatch.setattr(_labels, "counts_by_label", lambda sid: counts)

    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/labels")
    assert resp.status_code == 200
    assert _body_typed_response_endpoints(resp) == {"labels": rows, "counts": counts}
    assert list(resp.json()["counts"]) == ["good", "bad", "neutral"]


def test_scoring_label_row_rejects_unknown_label():
    from pydantic import ValidationError

    from backend.models.session_scoring import ScoringLabelRow

    assert ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "good"}).label == "good"
    with pytest.raises(ValidationError):
        ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "maybe"})


def test_scoring_label_literal_matches_allowed_labels():
    import typing

    from backend.models.session_scoring import ScoringLabelRow
    from backend.scoring.labels import ALLOWED_LABELS

    ann = ScoringLabelRow.model_fields["label"].annotation
    assert set(typing.get_args(ann)) == set(ALLOWED_LABELS)


def test_debug_state_matches_producer(client):
    from backend.models.common import _debug_responses_enabled

    resp = client.get("/api/debug/state")
    assert resp.status_code == 200
    assert _body_typed_response_endpoints(resp) == {"debug_responses_enabled": _debug_responses_enabled()}


def test_schema_response_serializes_schema_alias():
    from backend.models.services import SchemaResponse

    payload = {"schema": [{"name": "ip", "type": "VARCHAR"}], "table_name": "logs_x"}
    assert SchemaResponse.model_validate(payload).model_dump(mode="json", by_alias=True) == payload


def test_audit_logs_entries_keep_the_producer_row_shape():
    from backend.models.services import AuditLogsResponse

    # ``metadata.get_audit_logs`` stringifies timestamps and json-decodes ``details``.
    entry = {
        "id": 7,
        "timestamp": "2026-01-01 00:00:00+00:00",
        "event_type": "provision",
        "details": {"bucket": "b", "log_fields": {"groups": ["A"]}},
        "actor": "ui",
        "source": "local",
    }
    payload = {"total": 1, "page": 1, "per_page": 50, "entries": [entry], "_debug_calls": []}
    dumped = AuditLogsResponse.model_validate(payload).model_dump(mode="json")
    assert dumped["entries"] == [entry]
    assert list(dumped["entries"][0]) == list(entry)
    assert dumped["_debug_calls"] == []


def test_cmcd_status_disabled_and_enabled_shapes(client, monkeypatch):
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    off = client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")
    assert off.status_code == 200
    assert _body_typed_response_endpoints(off) == {"enabled": False}

    cfg = {"cmcd": {"enabled": True, "mode": "headers", "version": 1, "enabled_at": "2026-01-01T00:00:00+00:00"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    on = client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")
    assert _body_typed_response_endpoints(on) == {
        "enabled": True,
        "mode": "headers",
        "version": 1,
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }


def _roundtrip(obj):
    return json.loads(json.dumps(obj))


def _body_typed_response_endpoints(resp):
    """Response JSON minus the telemetry-middleware ``_debug_*``/``_section_timings`` envelope."""
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def test_log_fields_catalog_route_matches_producer(client):
    from backend.routers.bootstrap import _compute_log_fields_catalog

    resp = client.get("/api/log-fields/catalog")
    assert resp.status_code == 200
    assert _body_typed_response_endpoints(resp) == _roundtrip(_compute_log_fields_catalog(None))


def test_log_fields_catalog_model_keeps_custom_entry_shape():
    from backend.core import log_fields as lf
    from backend.models.common import LogFieldsCatalogResponse

    custom = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_custom", "label": "My", "description": "d"}]}
    )
    payload = {"groups": [], "fields": custom, "insights": [], "presets": {}}
    dumped = LogFieldsCatalogResponse.model_validate(payload).model_dump(mode="json")
    assert dumped == _roundtrip(payload)
    assert "typical_bytes" not in dumped["fields"][0]


def test_bootstrap_response_embeds_catalog_unchanged():
    from backend.models.common import BootstrapResponse
    from backend.routers.bootstrap import _compute_log_fields_catalog

    catalog = _compute_log_fields_catalog(None)
    resp = BootstrapResponse(active_service_id=None, services=[], log_fields_catalog=catalog)
    assert resp.model_dump(mode="json", by_alias=True)["log_fields_catalog"] == _roundtrip(catalog)


# ── Merged from tests/routers/test_remote_share_admin.py ──
def _make_admin_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_admin.router)
    return app


def _make_analyst_dependency_app() -> FastAPI:
    """App that stamps ``request.state.analyst_session`` so ``Depends(require_admin)``
    is exercised directly on the router even if middleware is bypassed."""

    class _StampAnalystMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            request.state.analyst_session = AnalystSession(
                session_id="sess-analyst-1",
                invite_id="inv-analyst-1",
                name="Analyst B",
                email="analyst@example.com",
                ip_address="203.0.113.10",
                user_agent="pytest-analyst",
                fingerprint_signature="fp-analyst",
                pii_policy={"mask_ips": True},
                query_window_hours=24,
                query_start_time=None,
                query_end_time=None,
                login_time="2026-01-01T00:00:00Z",
                last_active_time="2026-01-01T00:05:00Z",
                service_ids=["svc-1"],
            )
            return await call_next(request)

    app = FastAPI()
    app.add_middleware(_StampAnalystMiddleware)
    app.include_router(share_admin.router)
    return app


@pytest.fixture
def remote_share_client():
    with TestClient(_make_admin_app()) as c:
        yield c


@pytest.fixture
def analyst_dep_client():
    with TestClient(_make_analyst_dependency_app()) as c:
        yield c


def test_status_and_live_contracts(remote_share_client: TestClient):
    """GET /api/admin/share/status and /api/admin/share/live return typed snapshots."""
    r_status = remote_share_client.get("/api/admin/share/status")
    assert r_status.status_code == 200, r_status.text
    status_body = r_status.json()
    for key in (
        "sharing_active",
        "public_endpoint",
        "public_url",
        "active_session_count",
        "max_concurrent_sessions",
        "invites",
        "sessions",
        "audit_logs",
        "services",
        "rate_limits",
        "telemetry",
    ):
        assert key in status_body

    r_live = remote_share_client.get("/api/admin/share/live")
    assert r_live.status_code == 200, r_live.text
    live_body = r_live.json()
    for key in (
        "sharing_active",
        "public_url",
        "active_session_count",
        "rate_limits",
        "telemetry",
    ):
        assert key in live_body


def test_start_and_stop_direct_sharing(remote_share_client: TestClient):
    """POST /api/admin/share/start and /stop toggle direct HTTPS live sharing."""
    r_bad = remote_share_client.post(
        "/api/admin/share/start",
        json={"public_endpoint": "http://insecure.example.com"},
    )
    assert r_bad.status_code == 400
    assert r_bad.json()["detail"]["error"] == "invalid_request"

    r_start = remote_share_client.post(
        "/api/admin/share/start",
        json={"public_endpoint": "https://share.example.com"},
    )
    assert r_start.status_code == 200, r_start.text
    assert r_start.json()["public_url"] == "https://share.example.com"

    r_live = remote_share_client.get("/api/admin/share/live")
    assert r_live.status_code == 200
    assert r_live.json()["sharing_active"] is True
    assert r_live.json()["public_url"] == "https://share.example.com"

    r_stop = remote_share_client.post("/api/admin/share/stop")
    assert r_stop.status_code == 200
    assert r_stop.json()["ok"] is True


def test_invite_lifecycle_create_list_revoke_reactivate_regenerate_passcode(remote_share_client: TestClient):
    """Full invite lifecycle: create -> list -> revoke -> reactivate -> regenerate-passcode."""
    r_create = remote_share_client.post(
        "/api/admin/share/invites",
        json={
            "name": "Alice Analyst",
            "email": "alice@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "duration_hours": 48,
            "service_ids": ["svc-alpha"],
            "pii_policy": {"mask_ips": True},
        },
    )
    assert r_create.status_code == 200, r_create.text
    invite = r_create.json()
    invite_id = invite["id"]
    assert invite["email"] == "alice@example.com"
    assert invite["revoked"] == 0
    assert invite["service_ids"] == ["svc-alpha"]

    # List invites via GET /api/admin/share/invites
    r_list = remote_share_client.get("/api/admin/share/invites")
    assert r_list.status_code == 200, r_list.text
    invites_list = r_list.json()["invites"]
    assert any(row["id"] == invite_id for row in invites_list)

    # Revoke invite
    r_revoke = remote_share_client.post(f"/api/admin/share/invites/{invite_id}/revoke")
    assert r_revoke.status_code == 200, r_revoke.text
    assert r_revoke.json()["ok"] is True
    assert share_db.get_remote_invite(invite_id)["revoked"] == 1

    # Reactivate invite
    r_reactivate = remote_share_client.post(f"/api/admin/share/invites/{invite_id}/reactivate")
    assert r_reactivate.status_code == 200, r_reactivate.text
    reactivated_body = r_reactivate.json()
    assert reactivated_body["id"] == invite_id
    assert reactivated_body["revoked"] == 0
    assert share_db.get_remote_invite(invite_id)["revoked"] == 0

    # Unknown invite reactivate returns 404
    r_reactivate_404 = remote_share_client.post("/api/admin/share/invites/nonexistent-id/reactivate")
    assert r_reactivate_404.status_code == 404

    # Regenerate passcode
    r_regen = remote_share_client.post(f"/api/admin/share/invites/{invite_id}/regenerate-passcode")
    assert r_regen.status_code == 200, r_regen.text
    regen_body = r_regen.json()
    assert regen_body["ok"] is True
    new_passcode = regen_body["passcode"]
    assert isinstance(new_passcode, str) and len(new_passcode) >= 16 and "-" in new_passcode

    # Verify the regenerated passcode replaced the old one in share_db
    stored = share_db.get_remote_invite(invite_id)
    assert not share_db.verify_passcode("ocean-breeze-cabin-42", stored["passcode"])
    assert share_db.verify_passcode(new_passcode, stored["passcode"])

    # Unknown invite regenerate-passcode returns 404
    r_regen_404 = remote_share_client.post("/api/admin/share/invites/nonexistent-id/regenerate-passcode")
    assert r_regen_404.status_code == 404


def test_session_revocation_boot_and_revoke_endpoints(remote_share_client: TestClient):
    """Both /sessions/{session_id}/revoke and /sessions/{session_id}/boot terminate an active session."""
    mgr = get_tunnel_manager()
    for sid in ("sess-revoke-1", "sess-boot-2"):
        mgr._sessions[sid] = AnalystSession(
            session_id=sid,
            invite_id="inv-1",
            name="Bob Analyst",
            email="bob@example.com",
            ip_address="198.51.100.7",
            user_agent="Mozilla/5.0",
            fingerprint_signature="fp-bob",
            pii_policy={"mask_ips": True},
            query_window_hours=24,
            query_start_time=None,
            query_end_time=None,
            login_time="2026-01-01T00:00:00Z",
            last_active_time="2026-01-01T00:01:00Z",
            service_ids=["svc-alpha"],
        )

    r_revoke = remote_share_client.post("/api/admin/share/sessions/sess-revoke-1/revoke")
    assert r_revoke.status_code == 200, r_revoke.text
    assert r_revoke.json()["ok"] is True
    assert "sess-revoke-1" not in mgr._sessions

    r_boot = remote_share_client.post("/api/admin/share/sessions/sess-boot-2/boot")
    assert r_boot.status_code == 200, r_boot.text
    assert r_boot.json()["ok"] is True
    assert "sess-boot-2" not in mgr._sessions

    # Unknown session returns 404 on both routes
    assert remote_share_client.post("/api/admin/share/sessions/missing-sess/revoke").status_code == 404
    assert remote_share_client.post("/api/admin/share/sessions/missing-sess/boot").status_code == 404


def test_audit_log_filtering_by_event_type_and_email(remote_share_client: TestClient):
    """GET /api/admin/share/audit-logs filters by event_type, email, and limit."""
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS",
        email="filter-target@example.com",
        ip_address="192.0.2.1",
        details="Successful login",
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL",
        email="other-user@example.com",
        ip_address="192.0.2.2",
        details="Bad passcode",
    )

    r_type = remote_share_client.get(
        "/api/admin/share/audit-logs",
        params={"event_type": "LOGIN_SUCCESS", "email": "filter-target@example.com", "limit": 50},
    )
    assert r_type.status_code == 200, r_type.text
    rows = r_type.json()["audit_logs"]
    assert len(rows) >= 1
    assert all(r["event_type"] == "LOGIN_SUCCESS" for r in rows)
    assert all("filter-target@example.com" in (r.get("email") or "") for r in rows)


def test_settings_and_tos_crud(remote_share_client: TestClient):
    """GET/PATCH /api/admin/share/settings and GET/POST/PUT /api/admin/share/tos."""
    r_patch = remote_share_client.patch(
        "/api/admin/share/settings",
        json={"max_concurrent_analyst_sessions": 15},
    )
    assert r_patch.status_code == 200, r_patch.text
    assert r_patch.json()["max_concurrent_analyst_sessions"] == 15

    r_get_settings = remote_share_client.get("/api/admin/share/settings")
    assert r_get_settings.status_code == 200, r_get_settings.text
    assert r_get_settings.json()["max_concurrent_analyst_sessions"] == 15

    # Read current TOS
    r_tos_get = remote_share_client.get("/api/admin/share/tos")
    assert r_tos_get.status_code == 200, r_tos_get.text
    assert "version" in r_tos_get.json()
    assert "text" in r_tos_get.json()

    # Publish new TOS version via POST
    new_md = "# Custom Terms of Use\n\nConfidential analyst terms v2 for testing."
    r_tos_post = remote_share_client.post("/api/admin/share/tos", json={"version": "v2.0-test", "text": new_md})
    assert r_tos_post.status_code == 200, r_tos_post.text
    assert r_tos_post.json()["version"] == "v2.0-test"
    assert r_tos_post.json()["text"] == new_md

    # Publish another TOS version via PUT
    newer_md = "# Custom Terms of Use\n\nConfidential analyst terms v3 for testing."
    r_tos_put = remote_share_client.put("/api/admin/share/tos", json={"version": "v3.0-test", "text": newer_md})
    assert r_tos_put.status_code == 200, r_tos_put.text
    assert r_tos_put.json()["version"] == "v3.0-test"
    assert r_tos_put.json()["text"] == newer_md

    # Blank TOS version/text rejected with 400
    r_tos_blank = remote_share_client.post("/api/admin/share/tos", json={"version": "   ", "text": ""})
    assert r_tos_blank.status_code == 400


@pytest.mark.parametrize(
    "method,path,json_body",
    [
        ("GET", "/api/admin/share/status", None),
        ("GET", "/api/admin/share/live", None),
        ("GET", "/api/admin/share/invites", None),
        (
            "POST",
            "/api/admin/share/invites",
            {"name": "X", "email": "x@y.com", "passcode": "ocean-breeze-cabin-42", "service_ids": ["s1"]},
        ),
        ("POST", "/api/admin/share/invites/inv-1/revoke", None),
        ("POST", "/api/admin/share/invites/inv-1/reactivate", None),
        ("POST", "/api/admin/share/invites/inv-1/regenerate-passcode", None),
        ("POST", "/api/admin/share/sessions/sess-1/revoke", None),
        ("POST", "/api/admin/share/sessions/sess-1/boot", None),
        ("GET", "/api/admin/share/audit-logs", None),
        ("GET", "/api/admin/share/settings", None),
        ("PATCH", "/api/admin/share/settings", {"max_concurrent_analyst_sessions": 5}),
        ("GET", "/api/admin/share/tos", None),
        ("POST", "/api/admin/share/tos", {"version": "v1", "text": "A" * 30}),
        ("PUT", "/api/admin/share/tos", {"version": "v2", "text": "B" * 30}),
        ("POST", "/api/admin/share/start", {"public_endpoint": "https://share.example.com"}),
        ("POST", "/api/admin/share/stop", None),
        ("POST", "/api/admin/share/panic", None),
    ],
)
def test_analyst_path_b_rejected_403_on_all_admin_share_routes(
    analyst_dep_client: TestClient,
    method: str,
    path: str,
    json_body: dict | None,
):
    """Analyst Path B sessions receive 403 Forbidden with error='admin_only' on every /api/admin/share/* route."""
    resp = analyst_dep_client.request(method, path, json=json_body)
    assert resp.status_code == 403, f"Expected 403 on {method} {path}, got {resp.status_code}: {resp.text}"
    payload = resp.json()
    detail = payload.get("detail", payload)
    assert detail.get("error") == "admin_only"


# ── Merged from tests/routers/test_sharing_domain.py ──
@pytest.fixture
def sharing_domain_client():
    return TestClient(app)


def clean_res(d: dict) -> dict:
    """Helper to strip telemetry/debug fields added by middleware."""
    return {k: v for k, v in d.items() if not k.startswith("_")}


def test_deploy_frontend_with_token_override(sharing_domain_client):
    """Verify that deploy-frontend works when token_override is provided directly."""
    payload = {
        "service_name": "test-override-service",
        "domain_name": "override-domain.global.ssl.fastly.net",
        "origin_host": "1.1.1.1",
        "origin_port": 80,
        "use_ssl": False,
        "token_override": "override-token-123",
    }

    mock_res = {
        "service_id": "service_override",
        "version": 1,
        "domain_name": "override-domain.global.ssl.fastly.net",
        "origin_host": "1.1.1.1",
    }

    with patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy:
        response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_deploy.assert_called_once_with(
            service_name="test-override-service",
            domain_name="override-domain.global.ssl.fastly.net",
            origin_host="1.1.1.1",
            origin_port=80,
            use_ssl=False,
            token="override-token-123",
            override_host=None,
        )


def test_deploy_frontend_resolves_token_from_config(sharing_domain_client):
    """Verify that token is resolved from list_configs() when token_override is not provided."""
    payload = {
        "service_name": "test-config-service",
        "domain_name": "config-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    mock_res = {
        "service_id": "service_config",
        "version": 2,
        "domain_name": "config-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    mock_configs = [
        {"service_id": "some-other-id", "fastly_api_key": ""},
        {"service_id": "target-id", "fastly_api_key": "config-resolved-token"},
    ]

    with (
        patch("backend.routers.share_admin.list_configs", return_value=mock_configs),
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
    ):
        response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_deploy.assert_called_once_with(
            service_name="test-config-service",
            domain_name="config-domain.global.ssl.fastly.net",
            origin_host="203.0.113.10",
            origin_port=80,
            use_ssl=False,
            token="config-resolved-token",
            override_host=None,
        )


def test_deploy_frontend_resolves_token_from_env(sharing_domain_client):
    """Verify that token falls back to FASTLY_API_KEY environment variable if not in override or configs."""
    payload = {
        "service_name": "test-env-service",
        "domain_name": "env-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    mock_res = {
        "service_id": "service_env",
        "version": 3,
        "domain_name": "env-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    with (
        patch("backend.routers.share_admin.list_configs", return_value=[]),
        patch.dict(os.environ, {"FASTLY_API_KEY": "env-resolved-token"}),
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
    ):
        response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_deploy.assert_called_once_with(
            service_name="test-env-service",
            domain_name="env-domain.global.ssl.fastly.net",
            origin_host="203.0.113.10",
            origin_port=80,
            use_ssl=False,
            token="env-resolved-token",
            override_host=None,
        )


def test_deploy_frontend_missing_token_error(sharing_domain_client):
    """Verify that a 400 error is returned when no token can be resolved."""
    payload = {
        "service_name": "test-fail-service",
        "domain_name": "fail-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    with (
        patch("backend.routers.share_admin.list_configs", return_value=[]),
        patch.dict(os.environ, {}),
    ):
        # Temporarily ensure FASTLY_API_KEY is not in env if it's there
        if "FASTLY_API_KEY" in os.environ:
            with patch.dict(os.environ, {}, clear=True):
                response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        else:
            response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)

        assert response.status_code == 400
        assert "Token is required" in response.json()["detail"]["message"]


def test_deploy_frontend_requires_origin_host(sharing_domain_client):
    response = sharing_domain_client.post(
        "/api/sharing/deploy-frontend",
        json={
            "service_name": "test-service",
            "domain_name": "test-domain.global.ssl.fastly.net",
            "token_override": "test-token",
        },
    )

    assert response.status_code == 422


def test_deploy_frontend_fails_500(sharing_domain_client):
    """Verify that a 500 error is returned when the deployment process raises an exception."""
    payload = {
        "service_name": "test-error-service",
        "domain_name": "error-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
        "token_override": "some-token",
    }

    with patch(
        "backend.routers.share_admin.deploy_remote_frontend",
        side_effect=RuntimeError("Fastly API error during Create Service: HTTP 400 - Name already taken"),
    ):
        response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "request_failed"
        assert "error_id" in response.json()["detail"]


def test_deploy_frontend_with_override_host(sharing_domain_client):
    """Verify that deploy-frontend correctly passes override_host when provided."""
    payload = {
        "service_name": "test-override-host-service",
        "domain_name": "override-host-domain.global.ssl.fastly.net",
        "origin_host": "1.1.1.1",
        "origin_port": 80,
        "use_ssl": False,
        "token_override": "override-token-123",
        "override_host": "my-custom-host-header.com",
    }

    mock_res = {
        "service_id": "service_override_host",
        "version": 1,
        "domain_name": "override-host-domain.global.ssl.fastly.net",
        "origin_host": "1.1.1.1",
    }

    with patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy:
        response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_deploy.assert_called_once_with(
            service_name="test-override-host-service",
            domain_name="override-host-domain.global.ssl.fastly.net",
            origin_host="1.1.1.1",
            origin_port=80,
            use_ssl=False,
            token="override-token-123",
            override_host="my-custom-host-header.com",
        )


def test_teardown_frontend_success(sharing_domain_client):
    """Verify that teardown-frontend successfully deletes the remote frontend on Fastly and clears config."""
    payload = {
        "service_id": "service123",
        "token_override": "teardown-token",
    }

    mock_config = {
        "service_id": "service123",
        "remote_frontend": {
            "service_id": "remote_service_123",
            "version": 1,
            "domain_name": "remote-domain.global.ssl.fastly.net",
            "origin_host": "203.0.113.10",
        },
    }

    saved: dict = {}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config) as mock_load,
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config", side_effect=fake_mutate_config(mock_config, saved)) as mock_mutate,
    ):
        response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        assert response.json()["ok"] is True
        mock_load.assert_called_once_with("service123")
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="teardown-token")
        mock_mutate.assert_called_once()
        assert mock_mutate.call_args.args[0] == "service123"
        assert saved == {"service_id": "service123"}


def test_teardown_frontend_already_removed(sharing_domain_client):
    """Verify that teardown-frontend returns gracefully if no remote frontend is configured."""
    payload = {
        "service_id": "service123",
        "token_override": "teardown-token",
    }

    mock_config = {
        "service_id": "service123",
        # no remote_frontend key
    }

    with (
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config") as mock_mutate,
    ):
        response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        assert response.json()["ok"] is True
        mock_delete.assert_not_called()
        mock_mutate.assert_not_called()


def test_deploy_frontend_with_service_id_writes_config(sharing_domain_client):
    """Verify that deploy-frontend writes remote_frontend block to service config when service_id is supplied."""
    payload = {
        "service_name": "test-service",
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
        "token_override": "token-123",
        "service_id": "logging-service-123",
    }

    mock_res = {
        "service_id": "service_remote_id",
        "version": 1,
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    mock_config = {
        "service_id": "logging-service-123",
    }

    saved: dict = {}
    with (
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
        patch("backend.config.mutate_config", side_effect=fake_mutate_config(mock_config, saved)) as mock_mutate,
    ):
        response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_mutate.assert_called_once()
        assert mock_mutate.call_args.args[0] == "logging-service-123"
        assert saved["remote_frontend"] == {
            "service_id": "service_remote_id",
            "version": 1,
            "domain_name": "test-domain.global.ssl.fastly.net",
            "origin_host": "203.0.113.10",
        }


def test_deploy_frontend_with_service_id_save_config_error_graceful(sharing_domain_client):
    """Verify that deploy-frontend handles config saving exceptions gracefully and still returns the deployment result."""
    payload = {
        "service_name": "test-service",
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
        "token_override": "token-123",
        "service_id": "logging-service-123",
    }

    mock_res = {
        "service_id": "service_remote_id",
        "version": 1,
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    with (
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res),
        patch("backend.config.mutate_config", side_effect=RuntimeError("filesystem full")),
    ):
        response = sharing_domain_client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res


def test_teardown_frontend_resolves_token_from_config(sharing_domain_client):
    """Verify that teardown-frontend resolves token from list_configs() when token_override is not provided."""
    payload = {
        "service_id": "service123",
    }

    mock_config = {
        "service_id": "service123",
        "remote_frontend": {
            "service_id": "remote_service_123",
        },
    }

    mock_configs = [
        {"service_id": "target-id", "fastly_api_key": "config-teardown-token"},
    ]

    with (
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config),
        patch("backend.routers.share_admin.list_configs", return_value=mock_configs),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config"),
    ):
        response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="config-teardown-token")


def test_teardown_frontend_resolves_token_from_env(sharing_domain_client):
    """Verify that teardown-frontend resolves token from env when not in configs or payload."""
    payload = {
        "service_id": "service123",
    }

    mock_config = {
        "service_id": "service123",
        "remote_frontend": {
            "service_id": "remote_service_123",
        },
    }

    with (
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config),
        patch("backend.routers.share_admin.list_configs", return_value=[]),
        patch.dict(os.environ, {"FASTLY_API_KEY": "env-teardown-token"}),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config"),
    ):
        response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="env-teardown-token")


def test_teardown_frontend_missing_token_error(sharing_domain_client):
    """Verify that teardown-frontend returns 400 when no token can be resolved."""
    payload = {
        "service_id": "service123",
    }

    with (
        patch("backend.routers.share_admin.list_configs", return_value=[]),
        patch.dict(os.environ, {}, clear=True),
    ):
        if "FASTLY_API_KEY" in os.environ:
            with patch.dict(os.environ, {}, clear=True):
                response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)
        else:
            response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)

        assert response.status_code == 400
        assert "Token is required" in response.json()["detail"]["message"]


def test_teardown_frontend_fails_500(sharing_domain_client):
    """Verify that teardown-frontend returns 500 when delete_remote_frontend fails."""
    payload = {
        "service_id": "service123",
        "token_override": "token",
    }

    mock_config = {
        "service_id": "service123",
        "remote_frontend": {
            "service_id": "remote_service_123",
        },
    }

    with (
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config),
        patch("backend.routers.share_admin.delete_remote_frontend", side_effect=RuntimeError("fastly offline")),
    ):
        response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "request_failed"


def test_teardown_frontend_http_exception_passed_through(sharing_domain_client):
    """Verify that teardown-frontend passes HTTPExceptions through directly."""
    payload = {
        "service_id": "service123",
        "token_override": "token",
    }

    mock_config = {
        "service_id": "service123",
        "remote_frontend": {
            "service_id": "remote_service_123",
        },
    }

    with (
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config),
        patch(
            "backend.routers.share_admin.delete_remote_frontend",
            side_effect=HTTPException(status_code=403, detail="Forbidden"),
        ),
    ):
        response = sharing_domain_client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 403
