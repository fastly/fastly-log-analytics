"""Security / RBAC scope enforcement, debug ring-buffer, typed response contracts, and remote share admin tests."""

from __future__ import annotations

import datetime as dt
import json
import os
import typing
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
from backend.core import log_fields as lf
from backend.core import metadata as metadata_db
from backend.core import share_db
from backend.main import _APP_VERSION, app
from backend.models import rum as rum_models
from backend.models.common import (
    BootstrapDebugState,
    BootstrapPopGeo,
    BootstrapResponse,
    LogFieldCatalogEntry,
    LogFieldsCatalogResponse,
    _debug_responses_enabled,
)
from backend.models.provision import (
    NgwafWorkspacesResponse,
    ProvisionCheckConfigResponse,
    ProvisionLakeInfoResponse,
    ProvisionValidateResponse,
)
from backend.models.security import SecurityBotRow
from backend.models.services import (
    AuditLogsResponse,
    CorrelateResponse,
    LogFieldAuditResponse,
    RealtimeSeedResponse,
    SchemaResponse,
    WizardStateResponse,
    WizardStepResponse,
)
from backend.models.session_scoring import (
    ScoringComplianceRow,
    ScoringDistributionRow,
    ScoringEnforceStatusCodeState,
    ScoringEnforceThresholdResponse,
    ScoringExcludeRegexState,
    ScoringL2EnforceState,
    ScoringLabelDeleteResponse,
    ScoringLabelRow,
    ScoringStatusResponse,
    ScoringThresholdState,
    ScoringTopFlaggedRow,
)
from backend.routers import alerts as alerts_router
from backend.routers import session_scoring as session_scoring_router
from backend.routers import session_scoring_admin as scoring_admin
from backend.routers import share_admin
from backend.routers.bootstrap import _compute_log_fields_catalog
from backend.routers.control_room import TAB_STUBS
from backend.routers.services import core as services_core_router
from backend.scoring import labels as _labels
from backend.scoring.labels import ALLOWED_LABELS
from backend.utils import sqlite_profiler, telemetry
from backend.utils.remote_access import RemoteAccessMiddleware, _is_blocked_path
from backend.utils.tunnel.manager import AnalystSession, get_tunnel_manager
from tests._helpers.config_fakes import fake_mutate_config
from tests.conftest import MOCK_SERVICE_ID


def _body(resp, *, status: int = 200):
    """Assert HTTP status and return JSON with telemetry underscore keys stripped."""
    assert resp.status_code == status, resp.text
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def _roundtrip(obj: Any) -> Any:
    return json.loads(json.dumps(jsonable_encoder(obj)))


# ── Cross-tenant scope enforcement (alerts, views, custom fields, scoring labels) ──


@pytest.fixture
def app_with_session():
    """Mini app that injects a per-test analyst_session on request.state via X-Test-Session-Services."""
    mini = FastAPI()
    mini.include_router(alerts_router.router)
    mini.include_router(alerts_router.views_router)
    mini.include_router(services_core_router.router)
    mini.include_router(session_scoring_router.router)

    @mini.middleware("http")
    async def stamp_session(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        if sid is not None:
            services = [s for s in sid.split(",") if s]
            request.state.analyst_session = SimpleNamespace(session_id="test", service_ids=services, email="t@t")
        else:
            request.state.analyst_session = None
        return await call_next(request)

    return mini


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
def test_alerts_list_filters_to_analyst_scope_and_admin_sees_all(app_with_session):
    fake_alerts = [_fake_alert("a1", "svc-A"), _fake_alert("a2", "svc-B"), _fake_alert("a3", "svc-C")]
    with patch("backend.repositories.alerts.get_alerts", return_value=fake_alerts), TestClient(app_with_session) as c:
        r = c.get("/api/alerts/", headers={"x-test-session-services": "svc-A,svc-B"})
        assert r.status_code == 200, r.text
        assert [a["id"] for a in r.json()["data"]] == ["a1", "a2"]

        r_admin = c.get("/api/alerts/")
        assert r_admin.status_code == 200, r_admin.text
        assert len(r_admin.json()["data"]) == 3


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path,patch_target",
    [
        ("/api/alerts/svc-A", "backend.repositories.alerts.get_alerts"),
        ("/api/views/svc-A", "backend.repositories.alerts.get_views"),
    ],
)
def test_alerts_and_views_service_scope_checks(app_with_session, path: str, patch_target: str):
    out_of_scope_path = path.replace("svc-A", "svc-B")
    with TestClient(app_with_session) as c:
        r_deny = c.get(out_of_scope_path, headers={"x-test-session-services": "svc-A"})
        assert _scope_403(r_deny), r_deny.text

    with patch(patch_target, return_value=[]), TestClient(app_with_session) as c:
        r_ok = c.get(path, headers={"x-test-session-services": "svc-A"})
        assert r_ok.status_code == 200


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
    with patch("backend.repositories.alerts.get_views", return_value=[row]), TestClient(app_with_session) as c:
        r = c.get("/api/views/svc-anything")
    assert r.status_code == 200 and r.json() == [row]


@pytest.mark.security_regression
def test_alerts_and_views_create_rejects_unauthorized_analyst(app_with_session):
    alert_payload = {
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
    view_payload = {"service_id": "svc-B", "name": "v", "filters": {}, "columns": []}
    with TestClient(app_with_session) as c:
        r_alert = c.post("/api/alerts/", json=alert_payload, headers={"x-test-session-services": "svc-A"})
        assert r_alert.status_code == 403

        r_view = c.post("/api/views/", json=view_payload, headers={"x-test-session-services": "svc-A"})
        assert r_view.status_code in (403, 422)
        if r_view.status_code == 403:
            assert _scope_403(r_view)


@pytest.mark.security_regression
@pytest.mark.parametrize("view_exists", [False, True], ids=["nonexistent", "existing"])
def test_views_delete_rejects_unauthorized_analyst(app_with_session, view_exists: bool):
    """Finding 017: scope gate runs before any DB lookup so both existing and non-existent ids return 403."""
    view_row = {"id": "v-target", "service_id": "svc-B", "name": "n"} if view_exists else None
    with (
        patch("backend.repositories.alerts.get_view_by_id", return_value=view_row),
        TestClient(app_with_session) as c,
    ):
        r = c.delete("/api/views/v-target?service_id=svc-B", headers={"x-test-session-services": "svc-A"})
    assert _scope_403(r), r.text


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "method,path,body",
    [
        ("GET", "/api/services/svc-B/custom-fields", None),
        ("GET", "/api/services/svc-B/custom-fields/export", None),
        ("POST", "/api/services/svc-B/custom-fields", _VALID_CF_BODY),
        ("PATCH", "/api/services/svc-B/custom-fields/x_test_field", {"label": "Updated"}),
        ("DELETE", "/api/services/svc-B/custom-fields/x_test_field", None),
        ("POST", "/api/services/svc-B/custom-fields/import", {"custom_fields": [_VALID_CF_BODY]}),
        (
            "POST",
            "/api/services/svc-B/custom-fields/validate-vcl",
            {"vcl_log_expression": "req.http.X-Test", "collection_stage": "deliver"},
        ),
    ],
)
def test_custom_fields_rejects_unauthorized_analyst(app_with_session, method: str, path: str, body: dict | None):
    with TestClient(app_with_session) as c:
        r = c.request(method, path, json=body, headers={"x-test-session-services": "svc-A"})
    assert _scope_403(r), r.text


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "service_id,headers",
    [
        ("svc-anything", {}),
        ("svc-A", {"x-test-session-services": "svc-A"}),
    ],
    ids=["admin_unrestricted", "in_scope_analyst"],
)
def test_custom_fields_authorized_callers_reach_handler(app_with_session, service_id: str, headers: dict):
    with (
        patch("backend.utils.router_utils.load_service_config", return_value={"log_fields": {"custom_fields": []}}),
        TestClient(app_with_session) as c,
    ):
        r = c.get(f"/api/services/{service_id}/custom-fields", headers=headers)
    assert r.status_code != 403, r.text


@pytest.mark.security_regression
def test_scoring_labels_delete_scope_and_admin_bypass(app_with_session):
    with TestClient(app_with_session) as c:
        r_deny = c.delete(
            "/api/services/svc-B/scoring/labels/some-label-id",
            headers={"x-test-session-services": "svc-A"},
        )
    assert _scope_403(r_deny) and r_deny.json()["detail"]["service"] == "svc-B"

    with (
        patch("backend.scoring.labels.delete_label", return_value={"id": "x", "service_id": "svc-B"}),
        TestClient(app_with_session) as c,
    ):
        r_admin = c.delete("/api/services/svc-B/scoring/labels/x")
    assert r_admin.status_code != 403, r_admin.text


# ── Blocklist enforcement (H-1..H-4, S-1, S-2) ───────────────────────────────


@pytest.fixture
def app_with_blocklist():
    """Mini app whose middleware runs ``_is_blocked_path`` for analyst requests."""
    mini = FastAPI()

    @mini.middleware("http")
    async def rbac_gate(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        if sid is not None:
            services = [s for s in sid.split(",") if s]
            request.state.analyst_session = SimpleNamespace(session_id="test", service_ids=services, email="t@t")
            if _is_blocked_path(request.url.path):
                return JSONResponse(status_code=403, content={"error": "admin_only"})
        else:
            request.state.analyst_session = None
        return await call_next(request)

    for route in (
        "/api/usage/prefill",
        "/api/download",
        "/api/download-all",
        "/api/download-folder",
        "/api/cron-schedule",
        "/api/services/{service_id}/lake-info",
        "/api/services/{service_id}/scoring/config",
        "/api/services/{service_id}/scoring/status",
        "/api/services/{service_id}/scoring/audit",
        "/api/services/{service_id}/scoring/threshold",
        "/api/services/{service_id}/scoring/exclude-regex",
        "/api/services/{service_id}/scoring/enforce-status-code",
        "/api/services/{service_id}/scoring/labels",
        "/api/services/{service_id}/scoring/sessions/{sid}/events",
        "/api/services/{service_id}/scoring/top-flagged",
        "/api/services/{service_id}/realtime-stream",
        "/api/services/{service_id}/realtime-seed",
        "/api/services/{service_id}/control-room/{tab}",
        "/api/services/{service_id}/log-field-audit",
    ):
        mini.add_api_route(
            route,
            lambda service_id="svc-A", sid="", tab="": {"ok": True, "service_id": service_id, "sid": sid, "tab": tab},
            methods=["GET"],
        )

    return mini


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/usage/prefill",
        "/api/download-all",
        "/api/download-folder",
        "/api/download",
        "/api/services/svc-A/lake-info",
        "/api/cron-schedule",
        "/api/services/svc-A/log-field-audit",
        *(
            f"/api/services/svc-A/scoring{s}"
            for s in (
                "/config",
                "/status",
                "/audit",
                "/threshold",
                "/exclude-regex",
                "/enforce-status-code",
                "/l2-enforce",
            )
        ),
    ],
)
def test_admin_only_routes_blocked_for_analyst(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403, f"{path}: {r.status_code} {r.text}"
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
        "/api/services/svc-A/lake-info/",
        "/api/services/svc-A/log-field-audit/",
        *(
            f"/api/services/svc-A/scoring{s}/"
            for s in ("/config", "/status", "/audit", "/threshold", "/exclude-regex", "/enforce-status-code")
        ),
    ],
)
def test_admin_only_routes_resist_trailing_slash_bypass(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403, f"{path}: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/services/svc-A/scoring/labels",
        "/api/services/svc-A/scoring/sessions/sess-1/events",
        "/api/services/svc-A/scoring/top-flagged",
        "/api/services/svc-A/realtime-stream",
        "/api/services/svc-A/realtime-seed",
        "/api/services/svc-A/control-room/admin_health",
    ],
)
def test_analyst_allowed_reads_pass_blocklist(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200, f"{path}: {r.status_code} {r.text}"
    assert r.json()["service_id"] == "svc-A"


@pytest.mark.security_regression
def test_realtime_stream_still_needs_service_scope():
    assert not _is_blocked_path("/api/services/svc-B/realtime-stream")


# ── Debug endpoints (/api/debug SQLite ring-buffer & DEBUG_RESPONSES) ────────

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


def _check_fos(client, *, fail: bool = False):
    fake = MagicMock()
    if fail:
        fake.list_objects_v2.side_effect = RuntimeError("boom")
    else:
        fake.list_objects_v2.return_value = {"Contents": []}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _audit_logs(client):
    return client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _iceberg_calendar(client):
    from backend.deps import get_source

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
    pytest.param(lambda c: _check_fos(c, fail=False), id="provision-check-fos-ok"),
    pytest.param(lambda c: _check_fos(c, fail=True), id="provision-check-fos-failed"),
    pytest.param(_audit_logs, id="audit-logs"),
    pytest.param(_iceberg_calendar, id="iceberg-calendar"),
    pytest.param(_log_fields_get, id="service-log-fields"),
    pytest.param(_generate_viewer_key, id="generate-viewer-key"),
]

_is_remote_flag = False
_original_getattr = starlette.datastructures.State.__getattr__


def _mock_state_getattr(self, key: str):
    if key == "is_remote":
        return _is_remote_flag
    try:
        return _original_getattr(self, key)
    except (AttributeError, KeyError):
        raise AttributeError(f"'State' object has no attribute '{key}'")


@pytest.mark.security_regression
class TestDebugEndpoints:
    @pytest.fixture(autouse=True)
    def _isolated_buffer(self, monkeypatch):
        global _is_remote_flag
        _is_remote_flag = False
        sqlite_profiler.clear()
        monkeypatch.setattr(starlette.datastructures.State, "__getattr__", _mock_state_getattr)
        yield
        _is_remote_flag = False
        sqlite_profiler.clear()

    def test_recent_sqlite_lifecycle_and_filters(self):
        client = TestClient(app)
        empty = client.get("/api/debug/recent-sqlite").json()
        assert empty["queries"] == [] and empty["buffer_size"] == 0 and empty["buffer_cap"] > 0

        con1 = metadata_db.get_con("svc-a")
        con1.execute("SELECT 1")
        first = client.get("/api/debug/recent-sqlite").json()
        assert first["buffer_size"] > 0
        assert any("SELECT 1" in q["sql"] for q in first["queries"])

        midpoint_seq = first["last_seq"]
        con2 = metadata_db.get_con("svc-b")
        con2.execute("SELECT 2")
        con2.execute("SELECT 3")
        second = client.get(f"/api/debug/recent-sqlite?since_seq={midpoint_seq}").json()
        assert all(q["seq"] > midpoint_seq for q in second["queries"])

        limited = client.get("/api/debug/recent-sqlite?limit=2").json()
        assert len(limited["queries"]) <= 2

        clear_res = client.post("/api/debug/clear-sqlite")
        assert clear_res.status_code == 200 and clear_res.json()["ok"] is True
        assert sqlite_profiler.get_recent()["buffer_size"] == 0

    @pytest.mark.parametrize(
        "env_val,payload,kwargs,expected",
        [
            (
                "true",
                {"ok": True},
                {"gated": True, "is_cached": None, "inject_calls": True},
                {"ok": True, "_debug_calls": _CALLS},
            ),
            (None, {"ok": True}, {"gated": True, "is_cached": None, "inject_calls": True}, {"ok": True}),
            (
                None,
                {"ok": True, "_debug_calls": ["stale"], "_debug_queries": ["stale"]},
                {"gated": True, "is_cached": None, "inject_calls": True},
                {"ok": True},
            ),
            ("true", {"ok": True}, {"gated": True, "is_cached": True}, {"ok": True, "_is_cached": True}),
        ],
    )
    def test_gated_envelope_variants(self, monkeypatch, recorded_calls, env_val, payload, kwargs, expected):
        if env_val is None:
            monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
        else:
            monkeypatch.setenv("DEBUG_RESPONSES", env_val)
        assert telemetry.debug_envelope(dict(payload), **kwargs) == expected

    @pytest.mark.parametrize("call", _SITES)
    def test_route_emits_no_debug_envelope_when_debug_responses_is_off(self, client, monkeypatch, call):
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
        r = call(client)
        assert r.status_code == 200 and _debug_keys(r.json()) == []

    @pytest.mark.parametrize("call", _SITES)
    def test_route_emits_the_call_trace_when_debug_responses_is_on(self, client, monkeypatch, recorded_calls, call):
        monkeypatch.setenv("DEBUG_RESPONSES", "true")
        r = call(client)
        assert r.status_code == 200 and [c["path"] for c in r.json()["_debug_calls"]] == ["/probe"]

    def test_get_and_patch_debug_settings_and_remote_access(self):
        global _is_remote_flag
        client = TestClient(app)

        body = client.get("/api/admin/debug-settings").json()
        assert body["query_debug_visibility"] == "disabled" and body["api_call_debug_visibility"] == "disabled"

        patched = client.patch(
            "/api/admin/debug-settings",
            json={"query_debug_visibility": "both", "api_call_debug_visibility": "analysts"},
        ).json()
        assert patched["query_debug_visibility"] == "both" and patched["api_call_debug_visibility"] == "analysts"
        assert get_setting("query_debug_visibility") == "both"

        _is_remote_flag = True
        set_setting("query_debug_visibility", "disabled")
        res_blocked = client.get("/api/debug/recent-sqlite")
        assert res_blocked.status_code == 403
        assert res_blocked.json()["detail"]["error"] == "sql_debug_disabled"

        set_setting("query_debug_visibility", "both")
        assert client.get("/api/debug/recent-sqlite").status_code == 200

        res_clear = client.post("/api/debug/clear-sqlite")
        assert res_clear.status_code == 403 and res_clear.json()["detail"]["error"] == "admin_only"

        set_setting("query_debug_visibility", "disabled")
        set_setting("api_call_debug_visibility", "disabled")


# ── Typed response contracts (admin & endpoints) ────────────────────────────


def _wire_model(model_cls: type, payload: dict[str, Any]) -> str:
    return model_cls.model_validate(payload).model_dump_json(by_alias=True, exclude_unset=True)


def _assert_same_bytes(model_cls: type, payload: dict[str, Any]) -> None:
    assert json.loads(_wire_model(model_cls, payload)) == jsonable_encoder(payload)
    assert _wire_model(model_cls, payload) == json.dumps(
        jsonable_encoder(payload), separators=(",", ":"), ensure_ascii=False
    )


def test_admin_bot_sources_and_rdns_match_producer(client):
    meta = {"id": "arcjet", "entry_count": 42, "last_updated": "2026-10-11T00:00:00Z", "enabled": True}
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=meta):
        assert _body(client.post("/api/admin/bot-sources/arcjet/refresh")) == {"ok": True, "source": meta}

    results = [{"id": "a", "entry_count": 3}, {"id": "b", "failed": True, "error": "boom"}]
    expected = {"ok": False, "sources": results, "updated_count": 1, "failed_count": 1}
    for path in ("/api/admin/bot-sources/refresh", "/api/admin/bots/refresh"):
        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=results):
            assert _body(client.post(path)) == expected

    summary = {"resolved": 4, "errors": 1, "discovered": 7, "batch_limit": 100}
    with patch("backend.utils.rdns_cache.enrich_batch", return_value=summary) as enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=100")
    enrich.assert_called_once_with(limit=100)
    assert _body(resp) == {"ok": True, "summary": summary}


def test_duckdb_recycle_and_status_match_producer(client):
    mb = 1024 * 1024
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * mb),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
    ):
        assert _body(client.post("/api/admin/duckdb/recycle")) == {
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
        assert _body(client.post("/api/admin/duckdb/recycle?force=true")) == {
            "ok": True,
            "status": "recycled",
            "detail": "manual_admin: recycled 1/1",
            "rss_before_mb": 300.0,
            "rss_after_mb": 200.0,
            "freed_mb": 100.0,
        }

    status_body = _body(client.get("/api/admin/duckdb/status"))
    assert set(status_body) == {"ok", "memory", "barrier", "pools", "retired_pools"}
    assert set(status_body["memory"]) == {"current_rss_mb", "recycle_threshold_mb", "recycle_interval_min"}
    assert set(status_body["barrier"]) == {"is_active", "active_paths"}


def test_ngwaf_share_purge_and_calendar_match_producer(client, monkeypatch):
    stats = {"total_cached_bots": 2, "workspaces": {"w": None}, "top_bots": {"bot": 2}}
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-n"}),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync"),
        patch("backend.utils.ngwaf.get_cache_stats", return_value=stats),
    ):
        assert _body(client.post("/api/admin/ngwaf/sync/svc-n")) == {
            "ok": True,
            "service_id": "svc-n",
            "workspace_id": "ws-1",
            "stats": stats,
        }

    monkeypatch.setattr("backend.utils.ngwaf.get_cache_stats", lambda: dict(stats))
    assert _body(client.get("/api/admin/ngwaf/status")) == {"ok": True, **stats}

    monkeypatch.setattr("backend.utils.rdns_cache.get_stats", lambda: {"entries": 4, "hit_rate": 0.5})
    assert _body(client.get("/api/admin/rdns/stats")) == {"ok": True, "stats": {"entries": 4, "hit_rate": 0.5}}

    purge_stats = {
        "deleted_audit_logs": 3,
        "deleted_expired_invites": 1,
        "deleted_stale_sessions": 2,
        "deleted_claim_tokens": 0,
        "audit_retention_days": 90,
        "max_idle_session_days": 30,
    }
    with patch("backend.core.share_db.purge_all_share_records", return_value=purge_stats):
        assert _body(client.post("/api/admin/share/purge")) == {"ok": True, **purge_stats}

    calendar = {"2026-10-01": {"data_files": 4, "size_bytes": 0}, "2026-10-02": {"data_files": 9, "size_bytes": 0}}
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=calendar):
        assert _body(client.get("/api/admin/iceberg-calendar")) == calendar


def test_vcl_health_page_telemetry_and_health_match_producer(client):
    Snippet = namedtuple("Snippet", "name")
    snippets = [Snippet("Fastly Log Analytics - capture"), Snippet("Fastly Log Analytics - scoring")]
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-a", "admin_token": "tok"}),
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=7),
        patch("backend.provision.declarative.fastly_integration.fetch_snippets", return_value=snippets),
    ):
        assert _body(client.get("/api/admin/vcl-health?service_id=svc-a")) == {
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
        assert _body(client.get("/api/admin/vcl-health?service_id=nope")) == {
            "service_id": "nope",
            "error": "Service nope not configured",
            "is_clean": False,
        }

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

    assert _body(client.get("/api/health")) == {"status": "ok", "version": _APP_VERSION}
    with patch("backend.config.list_service_ids", return_value=[]):
        assert _body(client.get("/api/health?deep=1")) == {"status": "ok", "version": _APP_VERSION, "services": []}


def test_admin_ingest_actions_and_beacons_match_producer(client):
    for counters in (
        {"files_downloaded": 2, "rows_downloaded": 10, "files_removed": 0},
        {"error": "Could not load table: x", "files_downloaded": 0},
    ):
        with patch("backend.core.iceberg.sync_data", return_value=counters):
            resp = client.post(
                "/api/admin/backfill-window?start_time=2026-10-01T00:00:00Z&end_time=2026-10-01T02:00:00Z"
            )
        assert _body(resp) == counters

    started = {"ok": True, "message": "started", "run_id": 41}
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value=started):
        assert _body(client.post("/api/admin/commit-iceberg"), status=202) == started
        assert _body(client.post("/api/admin/ingest-logs")) == started

    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb.start_cron_run", return_value=77),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
    ):
        assert _body(client.post("/api/admin/rebuild-local-view"), status=202) == {
            "ok": True,
            "message": "Local view rebuild started.",
            "run_id": 77,
        }

    cfg = {"service_id": "svc-t", "remote_frontend": {"service_id": "remote-1"}}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=cfg),
        patch("backend.routers.share_admin.delete_remote_frontend"),
        patch("backend.config.mutate_config", return_value=cfg),
    ):
        resp = client.post("/api/sharing/teardown-frontend", json={"service_id": "svc-t", "token_override": "t"})
    assert _body(resp) == {"ok": True, "message": "Successfully deleted remote frontend service remote-1"}

    assert _body(client.post("/api/ux-events", json={"event": "column_reordered", "pathname": "/x"})) == {"ok": True}
    assert _body(client.post("/api/web-vitals", json={"id": "v1", "name": "LCP", "value": 1.2, "rating": "good"})) == {
        "ok": True
    }


def test_celery_status_and_control_room_tabs_match_producer(client, monkeypatch):
    for payload in (
        {
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
        },
        {
            "broker_reachable": False,
            "workers": {
                "active_tasks": {},
                "stats": {},
                "registered": {},
                "scheduled": {},
                "count": 0,
                "error": "down",
            },
            "queues": {},
            "inprocess_schedules": [],
            "ledger": {},
        },
    ):
        monkeypatch.setattr("backend.routers.admin.health.get_celery_status", lambda p=payload: p)
        assert _body(client.get("/api/admin/celery/status")) == _roundtrip(payload)

    for tab, stub in TAB_STUBS.items():
        assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}")) == stub, tab


@pytest.mark.parametrize(
    "model_cls,payload",
    [
        (RealtimeSeedResponse, {"ticks": [{"ts": 1, "rps": 2.5, "nested": {"a": [1]}}]}),
        (
            LogFieldAuditResponse,
            {
                "service_id": "svc",
                "enabled_fields": ["ip", "url"],
                "features": [{"name": "Insights", "required_fields": ["ip"], "status": "ok", "missing_fields": []}],
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
        (ScoringEnforceThresholdResponse, {"threshold": None, "enforced": False, "key": "enforce_threshold"}),
        (ScoringEnforceThresholdResponse, {"threshold": 70, "enforced": True, "key": "enforce_threshold"}),
        (ScoringDistributionRow, {"hour": "2026-01-01T00:00:00", "bucket": "0-25", "count": 4}),
        (ScoringComplianceRow, {"hour": "2026-01-01T00:00:00", "compliance": "ok", "count": 4}),
        (ScoringTopFlaggedRow, {"timestamp": "2026-01-01T00:00:00", "edge_sid": None, "edge_score": 90, "url": "/a"}),
        (BootstrapPopGeo, {"city": "Denver", "region": "co", "country": "us"}),
        (BootstrapPopGeo, {"city": "", "region": "", "country": ""}),
        (
            BootstrapDebugState,
            {
                "debug_responses_enabled": False,
                "query_debug_visibility": "disabled",
                "api_call_debug_visibility": "admins",
            },
        ),
        (
            ProvisionCheckConfigResponse,
            {"logging_service": {"ok": True, "details": "ok"}, "cdn_service": {"ok": False, "details": ""}},
        ),
        (
            NgwafWorkspacesResponse,
            {
                "workspaces": [{"id": "w", "name": "n"}],
                "error_hint": "h",
                "_debug_raw": "GET /x\n\n--- Response (200) ---",
            },
        ),
        (
            ProvisionValidateResponse,
            {"service_name": "s", "token_info": {"id": "1", "name": "t", "user_id": None, "type": "automation"}},
        ),
        (
            ProvisionLakeInfoResponse,
            {
                "ok": True,
                "table_exists": True,
                "info": {"table_name": "logs", "snapshots": 2, "data_files": 7, "size_bytes": 10, "region": None},
                "calendar": {"2026-01-01": {"data_files": 7, "size_bytes": 0}},
                "range": {"start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00"},
            },
        ),
        (SecurityBotRow, {"id": "gptbot", "name": "Gptbot", "category": "ai", "request_count": 12}),
        (SecurityBotRow, {"name": "Googlebot", "category": "search", "request_count": 5}),
        (
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
        ),
    ],
)
def test_response_models_wire_roundtrip(model_cls, payload):
    _assert_same_bytes(model_cls, payload)


def test_scoring_models_and_routes_match_producers(client, monkeypatch):
    _assert_same_bytes(ScoringL2EnforceState, scoring_admin._l2_enforce_unavailable())
    for kwargs in (
        {
            "scoring_enabled_at_raw": "1700000000",
            "l2_enforce_raw": "1",
            "l2_enabled_at_raw": "1700086400",
            "now_epoch": 1700200000,
        },
        {
            "scoring_enabled_at_raw": None,
            "l2_enforce_raw": None,
            "l2_enabled_at_raw": None,
            "now_epoch": 1700200000,
        },
    ):
        _assert_same_bytes(ScoringL2EnforceState, scoring_admin._build_l2_enforce_block(**kwargs))

    cfg: dict[str, Any] = {
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
    _assert_same_bytes(ScoringThresholdState, scoring_admin.scoring_threshold_get("svc"))
    _assert_same_bytes(ScoringExcludeRegexState, scoring_admin.scoring_exclude_regex_get("svc"))
    _assert_same_bytes(ScoringEnforceStatusCodeState, scoring_admin.scoring_enforce_status_code_get("svc"))

    cfg["scoring"] = {"enabled": True}
    _assert_same_bytes(ScoringThresholdState, scoring_admin.scoring_threshold_get("svc"))
    _assert_same_bytes(ScoringEnforceStatusCodeState, scoring_admin.scoring_enforce_status_code_get("svc"))

    with pytest.raises(ValidationError):
        ScoringTopFlaggedRow.model_validate({"edge_sid": "x", "edge_score": 1})
    with pytest.raises(ValidationError):
        ProvisionCheckConfigResponse.model_validate({"logging_service": {"ok": True, "details": "x"}})

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")) == {"enabled": False}

    cfg_full = {
        "service_id": MOCK_SERVICE_ID,
        "scoring": {"enabled": True, "request_secret": "s3cret", "custom_key": [1, 2], "deployed_vcl_sha": "v"},
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg_full)
    expected = session_scoring_router.scoring_status(MOCK_SERVICE_ID)
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert _body(resp) == expected and "request_secret" not in resp.json()
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/config"))["status"] == expected

    assert ScoringLabelDeleteResponse.model_validate({"status": "success", "id": "abc"}).model_dump(mode="json") == {
        "status": "success",
        "id": "abc",
    }

    ident_payload = {
        "enabled": True,
        "scoring_service_id": "svc",
        "scoring_service_name": "n",
        "scoring_domain": "d.example",
        "matrix_version": "2026-01-01",
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }
    assert set(ident_payload) <= set(ScoringStatusResponse.model_fields)
    assert (
        ScoringStatusResponse.model_validate(ident_payload).model_dump(mode="json", exclude_unset=True) == ident_payload
    )

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
    lbl_resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/labels")
    assert _body(lbl_resp) == {"labels": rows, "counts": counts}
    assert list(lbl_resp.json()["counts"]) == ["good", "bad", "neutral"]

    assert ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "good"}).label == "good"
    with pytest.raises(ValidationError):
        ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "maybe"})
    assert set(typing.get_args(ScoringLabelRow.model_fields["label"].annotation)) == set(ALLOWED_LABELS)


def test_catalog_bootstrap_schema_audit_and_cmcd_contracts(client, monkeypatch):
    payload = _compute_log_fields_catalog(None)
    assert payload["groups"] and payload["fields"] and payload["insights"] and payload["presets"]
    _assert_same_bytes(LogFieldsCatalogResponse, payload)
    assert _body(client.get("/api/log-fields/catalog")) == _roundtrip(payload)

    resp_boot = BootstrapResponse(active_service_id=None, services=[], log_fields_catalog=payload)
    assert resp_boot.model_dump(mode="json", by_alias=True)["log_fields_catalog"] == _roundtrip(payload)

    entries = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_field", "label": "My Field", "description": "d", "enabled": True}]}
    )
    assert entries
    for entry in entries:
        assert json.loads(_wire_model(LogFieldCatalogEntry, entry)) == jsonable_encoder(entry)
    custom_payload = {"groups": [], "fields": entries, "insights": [], "presets": {}}
    dumped_cat = LogFieldsCatalogResponse.model_validate(custom_payload).model_dump(mode="json")
    assert dumped_cat == _roundtrip(custom_payload) and "typical_bytes" not in dumped_cat["fields"][0]

    assert _body(client.get("/api/debug/state")) == {"debug_responses_enabled": _debug_responses_enabled()}

    schema_payload = {"schema": [{"name": "ip", "type": "VARCHAR"}], "table_name": "logs_x"}
    assert SchemaResponse.model_validate(schema_payload).model_dump(mode="json", by_alias=True) == schema_payload

    entry = {
        "id": 7,
        "timestamp": "2026-01-01 00:00:00+00:00",
        "event_type": "provision",
        "details": {"bucket": "b", "log_fields": {"groups": ["A"]}},
        "actor": "ui",
        "source": "local",
    }
    audit_payload = {"total": 1, "page": 1, "per_page": 50, "entries": [entry], "_debug_calls": []}
    dumped_audit = AuditLogsResponse.model_validate(audit_payload).model_dump(mode="json")
    assert dumped_audit["entries"] == [entry] and list(dumped_audit["entries"][0]) == list(entry)

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")) == {"enabled": False}

    cmcd_cfg = {"cmcd": {"enabled": True, "mode": "headers", "version": 1, "enabled_at": "2026-01-01T00:00:00+00:00"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cmcd_cfg)
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")) == cmcd_cfg["cmcd"]


def test_query_rows_and_optional_explain_plan(client):
    base_result = {
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
    expected_rows = _roundtrip(base_result["data"])
    with patch("backend.repositories.query.execute_query", return_value=dict(base_result)):
        resp = client.post("/api/query", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"sql": "SELECT 1"})
    body = resp.json()
    assert resp.status_code == 200 and body["data"] == expected_rows
    assert body["data"][0]["tz_ts"].endswith("+00:00") and body["data"][0]["amount"] == 12.5

    no_plan = {k: v for k, v in base_result.items() if k != "explain_plan"}
    with patch("backend.repositories.query.execute_query", return_value=no_plan):
        resp2 = client.post("/api/query", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"sql": "SELECT 1"})
    assert resp2.status_code == 200 and "explain_plan" not in resp2.json()


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
    model = getattr(rum_models, model_name)
    validated = model.model_validate(payload)
    assert validated.model_dump(mode="json") == payload
    assert validated.model_dump_json() == json.dumps(payload, separators=(",", ":"))


# ── Remote share admin & frontend deployment ────────────────────────────────


def _make_admin_app() -> FastAPI:
    mini = FastAPI()
    mini.add_middleware(RemoteAccessMiddleware)
    mini.include_router(share_admin.router)
    return mini


def _make_analyst_dependency_app() -> FastAPI:
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

    mini = FastAPI()
    mini.add_middleware(_StampAnalystMiddleware)
    mini.include_router(share_admin.router)
    return mini


@pytest.fixture
def remote_share_client():
    with TestClient(_make_admin_app()) as c:
        yield c


@pytest.fixture
def analyst_dep_client():
    with TestClient(_make_analyst_dependency_app()) as c:
        yield c


def test_share_status_live_start_stop_and_settings(remote_share_client: TestClient):
    status_body = _body(remote_share_client.get("/api/admin/share/status"))
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

    live_body = _body(remote_share_client.get("/api/admin/share/live"))
    for key in ("sharing_active", "public_url", "active_session_count", "rate_limits", "telemetry"):
        assert key in live_body

    r_bad = remote_share_client.post("/api/admin/share/start", json={"public_endpoint": "http://insecure.example.com"})
    assert r_bad.status_code == 400 and r_bad.json()["detail"]["error"] == "invalid_request"

    r_start = _body(
        remote_share_client.post("/api/admin/share/start", json={"public_endpoint": "https://share.example.com"})
    )
    assert r_start["public_url"] == "https://share.example.com"

    live_on = _body(remote_share_client.get("/api/admin/share/live"))
    assert live_on["sharing_active"] is True and live_on["public_url"] == "https://share.example.com"

    assert _body(remote_share_client.post("/api/admin/share/stop"))["ok"] is True

    # Settings + TOS CRUD
    assert (
        _body(remote_share_client.patch("/api/admin/share/settings", json={"max_concurrent_analyst_sessions": 15}))[
            "max_concurrent_analyst_sessions"
        ]
        == 15
    )
    assert _body(remote_share_client.get("/api/admin/share/settings"))["max_concurrent_analyst_sessions"] == 15

    tos_init = _body(remote_share_client.get("/api/admin/share/tos"))
    assert "version" in tos_init and "text" in tos_init

    for method, ver, text in (
        ("post", "v2.0-test", "# Custom Terms of Use\n\nConfidential analyst terms v2 for testing."),
        ("put", "v3.0-test", "# Custom Terms of Use\n\nConfidential analyst terms v3 for testing."),
    ):
        res = _body(getattr(remote_share_client, method)("/api/admin/share/tos", json={"version": ver, "text": text}))
        assert res["version"] == ver and res["text"] == text

    assert remote_share_client.post("/api/admin/share/tos", json={"version": "   ", "text": ""}).status_code == 400


def test_invite_session_and_audit_lifecycle(remote_share_client: TestClient):
    invite = _body(
        remote_share_client.post(
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
    )
    invite_id = invite["id"]
    assert invite["email"] == "alice@example.com" and invite["revoked"] == 0 and invite["service_ids"] == ["svc-alpha"]

    invites_list = _body(remote_share_client.get("/api/admin/share/invites"))["invites"]
    assert any(row["id"] == invite_id for row in invites_list)

    assert _body(remote_share_client.post(f"/api/admin/share/invites/{invite_id}/revoke"))["ok"] is True
    assert share_db.get_remote_invite(invite_id)["revoked"] == 1

    reactivated = _body(remote_share_client.post(f"/api/admin/share/invites/{invite_id}/reactivate"))
    assert reactivated["id"] == invite_id and reactivated["revoked"] == 0
    assert remote_share_client.post("/api/admin/share/invites/nonexistent-id/reactivate").status_code == 404

    regen_body = _body(remote_share_client.post(f"/api/admin/share/invites/{invite_id}/regenerate-passcode"))
    new_passcode = regen_body["passcode"]
    assert regen_body["ok"] is True and isinstance(new_passcode, str) and len(new_passcode) >= 16
    stored = share_db.get_remote_invite(invite_id)
    assert not share_db.verify_passcode("ocean-breeze-cabin-42", stored["passcode"])
    assert share_db.verify_passcode(new_passcode, stored["passcode"])
    assert remote_share_client.post("/api/admin/share/invites/nonexistent-id/regenerate-passcode").status_code == 404

    # Sessions revoke & boot
    mgr = get_tunnel_manager()
    for sid, action in (("sess-revoke-1", "revoke"), ("sess-boot-2", "boot")):
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
        assert _body(remote_share_client.post(f"/api/admin/share/sessions/{sid}/{action}"))["ok"] is True
        assert sid not in mgr._sessions
        assert remote_share_client.post(f"/api/admin/share/sessions/missing-sess/{action}").status_code == 404

    # Audit log filtering
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS", email="filter-target@example.com", ip_address="192.0.2.1", details="OK"
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="other-user@example.com", ip_address="192.0.2.2", details="Bad"
    )
    rows = _body(
        remote_share_client.get(
            "/api/admin/share/audit-logs",
            params={"event_type": "LOGIN_SUCCESS", "email": "filter-target@example.com", "limit": 50},
        )
    )["audit_logs"]
    assert rows and all(
        r["event_type"] == "LOGIN_SUCCESS" and "filter-target@example.com" in (r.get("email") or "") for r in rows
    )


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
    resp = analyst_dep_client.request(method, path, json=json_body)
    assert resp.status_code == 403, f"Expected 403 on {method} {path}, got {resp.status_code}: {resp.text}"
    payload = resp.json()
    assert payload.get("detail", payload).get("error") == "admin_only"


@pytest.mark.parametrize(
    "extra_payload,configs,env_token,expected_token,expected_override_host",
    [
        ({"token_override": "override-token-123"}, [], None, "override-token-123", None),
        (
            {},
            [{"service_id": "other", "fastly_api_key": ""}, {"service_id": "t", "fastly_api_key": "cfg-tok"}],
            None,
            "cfg-tok",
            None,
        ),
        ({}, [], "env-resolved-token", "env-resolved-token", None),
        (
            {"token_override": "override-token-123", "override_host": "my-custom-host-header.com"},
            [],
            None,
            "override-token-123",
            "my-custom-host-header.com",
        ),
    ],
    ids=["token_override", "config_token", "env_token", "with_override_host"],
)
def test_deploy_frontend_token_resolution_and_options(
    client,
    extra_payload: dict,
    configs: list[dict],
    env_token: str | None,
    expected_token: str,
    expected_override_host: str | None,
):
    payload = {
        "service_name": "test-svc",
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
        **extra_payload,
    }
    mock_res = {
        "service_id": "svc_1",
        "version": 1,
        "domain_name": payload["domain_name"],
        "origin_host": "203.0.113.10",
    }
    env_patch = {"FASTLY_API_KEY": env_token} if env_token else {}
    with (
        patch("backend.routers.share_admin.list_configs", return_value=configs),
        patch.dict(os.environ, env_patch, clear=env_token is None),
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
    ):
        resp = client.post("/api/sharing/deploy-frontend", json=payload)
    assert _body(resp) == mock_res
    mock_deploy.assert_called_once_with(
        service_name="test-svc",
        domain_name="test-domain.global.ssl.fastly.net",
        origin_host="203.0.113.10",
        origin_port=80,
        use_ssl=False,
        token=expected_token,
        override_host=expected_override_host,
    )


def test_deploy_frontend_config_persistence_and_error_paths(client):
    base_payload = {
        "service_name": "test-service",
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }
    mock_res = {
        "service_id": "service_remote_id",
        "version": 1,
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
    }

    # Missing token -> 400
    with (
        patch("backend.routers.share_admin.list_configs", return_value=[]),
        patch.dict(os.environ, {}, clear=True),
    ):
        r_no_tok = client.post("/api/sharing/deploy-frontend", json=base_payload)
        assert r_no_tok.status_code == 400 and "Token is required" in r_no_tok.json()["detail"]["message"]

    # Missing origin_host -> 422
    assert (
        client.post(
            "/api/sharing/deploy-frontend",
            json={"service_name": "s", "domain_name": "d.global.ssl.fastly.net", "token_override": "t"},
        ).status_code
        == 422
    )

    # Deployment RuntimeError -> 500
    with patch("backend.routers.share_admin.deploy_remote_frontend", side_effect=RuntimeError("Fastly API error")):
        r_500 = client.post("/api/sharing/deploy-frontend", json={**base_payload, "token_override": "some-token"})
        assert r_500.status_code == 500 and r_500.json()["detail"]["error"] == "request_failed"

    # Writes remote_frontend to service config when service_id is supplied
    saved: dict = {}
    mock_cfg = {"service_id": "logging-service-123"}
    svc_payload = {**base_payload, "token_override": "token-123", "service_id": "logging-service-123"}
    with (
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res),
        patch("backend.config.mutate_config", side_effect=fake_mutate_config(mock_cfg, saved)) as mock_mutate,
    ):
        assert _body(client.post("/api/sharing/deploy-frontend", json=svc_payload)) == mock_res
        mock_mutate.assert_called_once()
        assert saved["remote_frontend"] == mock_res

    # Config save failure is handled gracefully
    with (
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res),
        patch("backend.config.mutate_config", side_effect=RuntimeError("filesystem full")),
    ):
        assert _body(client.post("/api/sharing/deploy-frontend", json=svc_payload)) == mock_res


def test_teardown_frontend_variants_and_errors(client):
    mock_config = {
        "service_id": "service123",
        "remote_frontend": {
            "service_id": "remote_service_123",
            "version": 1,
            "domain_name": "remote-domain.global.ssl.fastly.net",
            "origin_host": "203.0.113.10",
        },
    }

    # 1. Explicit token_override deletes remote frontend and clears config key
    saved: dict = {}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=dict(mock_config)),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config", side_effect=fake_mutate_config(dict(mock_config), saved)),
    ):
        res = client.post(
            "/api/sharing/teardown-frontend", json={"service_id": "service123", "token_override": "teardown-token"}
        )
        assert res.status_code == 200 and res.json()["ok"] is True
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="teardown-token")
        assert saved == {"service_id": "service123"}

    # 2. Already removed -> no-op
    with (
        patch("backend.utils.router_utils.load_service_config", return_value={"service_id": "service123"}),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config") as mock_mutate,
    ):
        res = client.post("/api/sharing/teardown-frontend", json={"service_id": "service123", "token_override": "t"})
        assert res.status_code == 200 and res.json()["ok"] is True
        mock_delete.assert_not_called()
        mock_mutate.assert_not_called()

    # 3. Token resolution from config and env
    for configs, env_dict, expected_tok in (
        ([{"service_id": "t", "fastly_api_key": "cfg-td-token"}], {}, "cfg-td-token"),
        ([], {"FASTLY_API_KEY": "env-td-token"}, "env-td-token"),
    ):
        with (
            patch("backend.utils.router_utils.load_service_config", return_value=dict(mock_config)),
            patch("backend.routers.share_admin.list_configs", return_value=configs),
            patch.dict(os.environ, env_dict, clear=not env_dict),
            patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
            patch("backend.config.mutate_config"),
        ):
            res = client.post("/api/sharing/teardown-frontend", json={"service_id": "service123"})
            assert res.status_code == 200
            mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token=expected_tok)

    # 4. Missing token -> 400
    with (
        patch("backend.routers.share_admin.list_configs", return_value=[]),
        patch.dict(os.environ, {}, clear=True),
    ):
        r_400 = client.post("/api/sharing/teardown-frontend", json={"service_id": "service123"})
        assert r_400.status_code == 400 and "Token is required" in r_400.json()["detail"]["message"]

    # 5. RuntimeError -> 500, HTTPException -> passed through
    for exc, expected_status in (
        (RuntimeError("fastly offline"), 500),
        (HTTPException(status_code=403, detail="Forbidden"), 403),
    ):
        with (
            patch("backend.utils.router_utils.load_service_config", return_value=dict(mock_config)),
            patch("backend.routers.share_admin.delete_remote_frontend", side_effect=exc),
        ):
            r_err = client.post(
                "/api/sharing/teardown-frontend", json={"service_id": "service123", "token_override": "t"}
            )
            assert r_err.status_code == expected_status
