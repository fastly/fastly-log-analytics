from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import typing
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import starlette.datastructures
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.middleware.base import BaseHTTPMiddleware

import backend.core.metadata as metadata_db
import backend.core.metadata.usage_log as _cron_log
import backend.core.realtime as poller_mod
import backend.models.analytics as m_scoring
import backend.models.analytics as m_sec
import backend.models.analytics as rum_models
import backend.models.common as m_prov
import backend.models.common as m_svc
import backend.repositories.assets as _views_repo
import backend.routers.session_scoring as scoring_admin
import backend.scoring as _labels
import backend.utils.telemetry as statement_profiler
from backend import config as svcconfig
from backend.core import duckdb, share_db
from backend.core import log_fields as lf
from backend.core.metadata import usage_log
from backend.core.realtime import RealtimeMetricsPublisher, error_tick_payload
from backend.core.realtime import publisher as rt_publisher
from backend.core.request_context import get_con, get_service_id, get_source, require_admin, require_service_access
from backend.core.request_telemetry import query_registry
from backend.core.share_db import get_setting, set_setting
from backend.cron import scheduler as _sched
from backend.high_scale.archive import ServingWatermark
from backend.main import _APP_VERSION, app
from backend.models import common as m_common
from backend.repositories._base import _safe_table
from backend.routers import alerts as alerts_router
from backend.routers import bootstrap, share_admin
from backend.routers import control_room as cr_mod
from backend.routers import session_scoring as session_scoring_router
from backend.routers.bootstrap import _bootstrap_sync, _compute_log_fields_catalog, _startup_stub_response
from backend.routers.control_room import TAB_STUBS
from backend.routers.services import core as services_core_router
from backend.scoring import ALLOWED_LABELS
from backend.utils import telemetry, tunnel
from backend.utils.remote_access import RemoteAccessMiddleware, _is_blocked_path
from backend.utils.telemetry import set_page_load_id
from backend.utils.tunnel import AnalystSession, get_tunnel_manager
from tests._helpers.config_fakes import fake_mutate_config
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests.conftest import MOCK_SERVICE_ID

_SVC_HDR = {"x-fastly-service-id": MOCK_SERVICE_ID}


def _seed_active_service(monkeypatch, tmp_path, extra: dict | None = None, sid: str = MOCK_SERVICE_ID):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    svcconfig.save_config(sid, {"service_id": sid, **(extra or {})})


def _get(client, sid: str = MOCK_SERVICE_ID):
    return client.get("/api/bootstrap", headers={"x-fastly-service-id": sid})


def test_log_fields_catalog_endpoint(client):
    resp = client.get("/api/log-fields/catalog")
    assert resp.status_code == 200
    data = resp.json()
    assert {"groups", "fields", "presets", "insights"} <= set(data)
    assert {"minimal", "standard", "security"} <= set(data["presets"])


def test_insight_availability_endpoint(client, in_memory_duckdb, test_service_source):
    insert_mock_logs(
        in_memory_duckdb, _safe_table(test_service_source["name"]), generate_mock_logs(test_service_source, num_logs=1)
    )
    resp = client.get("/api/insight-availability", headers=_SVC_HDR)
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True and "insights" in data
    error_spikes = next(i for i in data["insights"] if i["id"] == "error_spikes")
    assert "id" in error_spikes and "available" in error_spikes


def test_bootstrap_endpoint_success(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)
    resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    assert data["active_service_id"] == MOCK_SERVICE_ID
    assert "pops" in data and "services" in data and data["settings"]["mask_ips"] is False


def test_startup_stub_still_withholds_warmup_dependent_fields():
    for is_remote in (False, True):
        fake_request = MagicMock()
        fake_request.state.is_remote = is_remote
        payload = _startup_stub_response(fake_request).model_dump(by_alias=True)
        assert payload["active_service_id"] is None and payload["services"] == [] and payload["schema"] is None
        assert payload["settings"]["initializing"] is True


def _login_analyst_and_bootstrap(client, tmp_path, monkeypatch, *, pii_policy, ngwaf_workspace_id=None):
    _seed_active_service(monkeypatch, tmp_path, {"ngwaf_workspace_id": ngwaf_workspace_id})
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    tunnel.reset_for_tests()
    invite = share_db.create_remote_invite(
        name="Test Analyst",
        email="analyst@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[MOCK_SERVICE_ID],
        pii_policy=pii_policy,
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    login = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert login.status_code == 200, login.text
    client.cookies.set("analyst_session_id", login.cookies.get("analyst_session_id"))
    resp = client.get(
        "/api/bootstrap",
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["settings"]["is_remote_analyst"] is True
    return body


@pytest.mark.parametrize("pii_policy,expected", [({"mask_ips": True}, True), (None, False)], ids=["masked", "unmasked"])
def test_bootstrap_mask_ips_for_analyst(client, tmp_path, monkeypatch, pii_policy, expected):
    try:
        body = _login_analyst_and_bootstrap(client, tmp_path, monkeypatch, pii_policy=pii_policy)
        assert body["settings"]["mask_ips"] is expected
    finally:
        share_db.close_all_connections()
        tunnel.reset_for_tests()


@pytest.mark.parametrize("workspace_id", [None, "", "mock-ngwaf-workspace"])
def test_bootstrap_ngwaf_capability_is_service_scoped(client, tmp_path, monkeypatch, workspace_id):
    _seed_active_service(
        monkeypatch,
        tmp_path,
        {
            "ngwaf_workspace_id": workspace_id,
            "log_fields": {"schema_version": 2, "groups": ["J"], "custom_fields": []},
        },
    )
    svcconfig.save_config("other-service", {"service_id": "other-service", "ngwaf_workspace_id": "other-workspace"})
    resp = _get(client)
    assert resp.status_code == 200 and resp.json()["ngwaf_configured"] is bool(workspace_id)


@pytest.mark.parametrize("workspace_id", [None, "mock-ngwaf-workspace"])
@pytest.mark.security_regression
def test_bootstrap_analyst_gets_ngwaf_capability_without_workspace_id(client, tmp_path, monkeypatch, workspace_id):
    try:
        body = _login_analyst_and_bootstrap(
            client, tmp_path, monkeypatch, pii_policy=None, ngwaf_workspace_id=workspace_id
        )
        assert body["ngwaf_configured"] is bool(workspace_id)
        assert all("ngwaf_workspace_id" not in service for service in body["services"])
    finally:
        share_db.close_all_connections()
        tunnel.reset_for_tests()


def test_bootstrap_exposes_active_log_field_ids(client, tmp_path, monkeypatch):
    lf_cfg = {"groups": lf.PRESETS["minimal"]["groups"], "field_overrides": {}}
    _seed_active_service(monkeypatch, tmp_path, {"log_fields": lf_cfg})
    resp = _get(client)
    assert resp.status_code == 200
    ids = resp.json()["active_log_field_ids"]
    assert isinstance(ids, list) and ids and set(ids) >= lf.resolve_enabled_fields(lf_cfg)


def test_bootstrap_falls_back_to_first_service_when_active_id_unknown(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path, sid="svc-real")
    resp = _get(client, sid="svc-deleted")
    assert resp.status_code == 200 and resp.json()["active_service_id"] == "svc-real"


def test_bootstrap_with_no_services_returns_null_active_id(client, tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    resp = client.get("/api/bootstrap")
    assert resp.status_code == 200
    data = resp.json()
    assert data["active_service_id"] is None and data["services"] == []


def test_bootstrap_uses_cached_schema_from_config_status(client, tmp_path, monkeypatch):
    cached_schema = [{"name": "timestamp", "type": "TIMESTAMP"}]
    _seed_active_service(monkeypatch, tmp_path, {"status": {"schema": cached_schema}})
    with (
        patch(
            "backend.services.service_manager.get_enriched_services",
            return_value=[{"service_id": MOCK_SERVICE_ID, "status": {"schema": cached_schema}}],
        ),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        resp = _get(client)
    assert resp.status_code == 200 and resp.json()["schema"] == cached_schema


def test_bootstrap_falls_back_to_live_schema_when_cache_empty(client, in_memory_duckdb, test_service_source):
    insert_mock_logs(
        in_memory_duckdb, _safe_table(test_service_source["name"]), generate_mock_logs(test_service_source, num_logs=1)
    )
    resp = _get(client)
    assert resp.status_code == 200 and isinstance(resp.json()["schema"], list)


def test_bootstrap_includes_custom_dashboard_cards_when_configured(client, tmp_path, monkeypatch):
    cfs = [
        {
            "name": "my_dashboard_field",
            "label": "Dashboard Field",
            "duckdb_type": "VARCHAR",
            "enabled": True,
            "show_in_dashboard": True,
        },
        {
            "name": "my_silent_field",
            "label": "Silent Field",
            "duckdb_type": "VARCHAR",
            "enabled": True,
            "show_in_dashboard": False,
        },
    ]
    _seed_active_service(monkeypatch, tmp_path, {"log_fields": {"schema_version": 2, "custom_fields": cfs}})
    resp = _get(client)
    assert resp.status_code == 200
    card_ids = {c["id"] for c in resp.json()["custom_dashboard_cards"]}
    assert "my_dashboard_field" in card_ids and "my_silent_field" not in card_ids


def test_bootstrap_includes_saved_views_for_active_service(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)

    def _fake_views(sid):
        assert sid == MOCK_SERVICE_ID
        base = {"service_id": sid, "filters_json": "[]", "start_time": None, "end_time": None, "page": "/dashboard"}
        return [{"id": "v1", "name": "Errors", **base}, {"id": "v2", "name": "Slow", **base}]

    monkeypatch.setattr(_views_repo, "get_views", _fake_views)
    resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    assert {v["id"] for v in data["views"]} == {"v1", "v2"}


def test_bootstrap_views_empty_when_no_active_service(client, tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    calls: list = []
    monkeypatch.setattr(_views_repo, "get_views", lambda sid: calls.append(sid) or [])
    resp = client.get("/api/bootstrap")
    assert resp.status_code == 200 and resp.json()["views"] == [] and calls == []


def test_bootstrap_views_survives_repo_error(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)
    monkeypatch.setattr(_views_repo, "get_views", MagicMock(side_effect=RuntimeError("simulated repo failure")))
    resp = _get(client)
    assert resp.status_code == 200 and resp.json()["views"] == []


def test_bootstrap_drops_cron_schedule_and_share_status_seeds(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)
    monkeypatch.setattr(_sched, "build_cron_schedule_payload", MagicMock(side_effect=AssertionError("P1#5")))
    monkeypatch.setattr(share_admin, "build_share_status", MagicMock(side_effect=AssertionError("P1#5")))
    resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    assert "cron_schedule" not in data and "share_status" not in data


def test_bootstrap_keeps_last_sync_share_banner_ops_overview_seeds(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)
    sync_info = {"started_at": "2026-06-29T00:00:00Z", "status": "success", "duration_s": 1.5}
    monkeypatch.setattr(_cron_log, "latest_cron_per_task", lambda sid: {"log_discovery": sync_info})
    fake_mgr = SimpleNamespace(is_sharing_active=lambda: True, public_url=lambda: "https://example.test/share-login")
    monkeypatch.setattr(tunnel, "get_tunnel_manager", lambda: fake_mgr)
    resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    assert data["last_sync"] == sync_info
    assert data["share_banner"] == {"sharing_active": True, "public_url": "https://example.test/share-login"}
    assert data["ops_overview"] is not None


def test_schema_endpoint_uses_cached_status_when_available(client, in_memory_duckdb, test_service_source):
    cached_schema = [{"name": "from_cache", "type": "VARCHAR"}]
    with patch("backend.config.get_status", return_value={"schema": cached_schema}):
        resp = client.get("/api/schema", headers=_SVC_HDR)
    assert resp.status_code == 200 and resp.json()["schema"] == cached_schema


def test_schema_endpoint_500s_when_get_schema_raises(client, test_service_source):
    with (
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.duckdb.get_schema", side_effect=RuntimeError("schema fetch failed")),
    ):
        resp = client.get("/api/schema", headers=_SVC_HDR)
    assert resp.status_code == 500 and "error" in resp.json()["detail"]


def test_log_fields_catalog_with_service_id_includes_field_limits_and_custom(client, tmp_path, monkeypatch):
    cf = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True, "label": "My Custom Field"}
    _seed_active_service(monkeypatch, tmp_path, {"log_fields": {"schema_version": 2, "custom_fields": [cf]}})
    resp = client.get("/api/log-fields/catalog", headers=_SVC_HDR)
    assert resp.status_code == 200
    custom_ids = [f["id"] for f in resp.json()["fields"] if f.get("group") == "custom"]
    assert "my_custom" in custom_ids


def test_log_fields_catalog_handles_missing_config_gracefully(client):
    resp = client.get("/api/log-fields/catalog", headers={"x-fastly-service-id": "nonexistent"})
    assert resp.status_code == 200 and {"fields", "groups"} <= set(resp.json())


def test_log_fields_catalog_500s_on_exception(client):
    with patch("backend.core.log_fields.get_catalog_for_api", side_effect=RuntimeError("oops")):
        assert client.get("/api/log-fields/catalog").status_code == 500


def test_insight_availability_500s_on_schema_failure(client):
    with patch("backend.core.duckdb.get_schema", side_effect=RuntimeError("schema gone")):
        assert client.get("/api/insight-availability", headers=_SVC_HDR).status_code == 500


def test_insight_availability_marks_unavailable_when_required_cols_missing(
    client, in_memory_duckdb, test_service_source
):
    in_memory_duckdb.execute(
        f"CREATE TABLE {_safe_table(test_service_source['name'])} (timestamp TIMESTAMP, status INTEGER)"
    )
    resp = client.get("/api/insight-availability", headers=_SVC_HDR)
    assert resp.status_code == 200 and any(not i["available"] for i in resp.json()["insights"])


def test_bootstrap_remote_unauth_stub_omits_section_timings():
    fake_request = MagicMock()
    fake_request.state.is_remote = True
    fake_request.state.analyst_session = None
    fake_request.cookies = {}
    payload = _bootstrap_sync(fake_request, None).model_dump(by_alias=True)
    assert payload["settings"]["is_remote_analyst"] is True and payload["settings"]["needs_login"] is True
    assert (payload.get("_section_timings") or payload.get("section_timings") or []) == []


def test_bootstrap_aligns_request_total_with_local_rows(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)
    monkeypatch.setattr(
        svcconfig, "get_status", lambda sid: {"local_rows": 7400000, "latest_log_at": "2026-06-29T12:00:00Z"}
    )

    class MockConnectionHolder:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return MagicMock()

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("backend.core.request_context._ConnectionHolder", MockConnectionHolder)
    monkeypatch.setattr("backend.core.storage.execute_with_stale_view_retry", lambda *a, **kw: (800, None))
    mock_duckdb_con = MagicMock()
    mock_duckdb_con.execute.return_value.fetchone.side_effect = [(33000,), (None,)]
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda src: mock_duckdb_con)

    resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    assert data["header_badge"]["rum"]["total_rows"] == 800
    assert data["header_badge"]["request"]["total_rows"] == 7399200
    assert data["header_badge"]["request"]["latest_log_at"] == "2026-06-29T12:00:00Z"
    assert data["header_badge"]["local_rows"] == 7400000

    scan_calls = mock_duckdb_con.execute.call_count
    again = _get(client)
    assert again.status_code == 200 and again.json()["header_badge"] == data["header_badge"]
    assert mock_duckdb_con.execute.call_count == scan_calls


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
@pytest.mark.parametrize("warm_cache", [False, True])
def test_bootstrap_header_badge_uses_live_sync_snapshot(client, tmp_path, monkeypatch, request_latest, warm_cache):
    _seed_active_service(monkeypatch, tmp_path)
    rows = 0 if request_latest is None else 20
    current_status = {
        "local_rows": rows,
        "latest_log_at": "2026-09-06T17:19:00Z",
        "earliest_log_at": "2026-09-04T00:00:00Z",
        "rum": {"latest_log_at": "2026-09-08T10:00:00Z", "total_rows": 10},
        "request": {"latest_log_at": request_latest, "total_rows": rows, "last_sync_at": None},
    }
    monkeypatch.setattr(svcconfig, "get_status", lambda sid: current_status.copy())
    monkeypatch.setattr(
        "backend.core.metadata.get_ingested_files_status_summary",
        lambda _: {"latest_file_name": "logs_2026-09-06T17-19-00.gz", "total_rows": 99},
    )
    if warm_cache:
        monkeypatch.setitem(
            bootstrap._header_badge_cache,
            MOCK_SERVICE_ID,
            (
                float("inf"),
                {"latest_log_at": "2026-09-06T17:19:00Z", "local_rows": 99},
                {"configured": True, "latest_log_at": "2026-09-06T17:19:00Z"},
                {},
            ),
        )
    resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    assert data["header_badge"]["latest_log_at"] == request_latest
    assert data["header_badge"]["request"]["latest_log_at"] == request_latest
    assert data["log_extents"]["latest_log_at"] == request_latest
    assert data["sync_status"]["latest_log_at"] == request_latest
    assert data["sync_status"]["latest_ingested_file_at"] == "2026-09-06 17:19:00"
    assert data["header_badge"]["rum"]["latest_log_at"] == "2026-09-08T10:00:00Z"
    assert data["header_badge"]["local_rows"] == rows


@pytest.mark.parametrize(
    "patch_target,field,subkey",
    [
        ("backend.utils.tunnel.get_tunnel_manager", "share_banner", None),
        ("backend.core.metadata.usage_log.get_cron_runs", "cron_runs_first_page", None),
        ("backend.core.metadata.usage_log.latest_cron_per_task", "last_sync", None),
        ("backend.scoring.list_labels", "scoring_labels", None),
        ("backend.core.metadata.count_slow_queries", "ops_overview", "slow_queries_count"),
    ],
)
def test_bootstrap_survives_individual_optional_failures(client, tmp_path, monkeypatch, patch_target, field, subkey):
    _seed_active_service(monkeypatch, tmp_path)
    with patch(patch_target, side_effect=RuntimeError("boom")):
        resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    assert data["active_service_id"] == MOCK_SERVICE_ID and "pops" in data and "services" in data
    if subkey:
        assert subkey not in (data.get(field) or {})
    else:
        assert data.get(field) is None


def test_bootstrap_survives_ops_overview_queries_summary_failure(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)
    with patch.object(query_registry, "summary", side_effect=RuntimeError("registry broke")):
        resp = _get(client)
    assert resp.status_code == 200 and "queries_summary" not in (resp.json().get("ops_overview") or {})


def test_bootstrap_survives_simultaneous_optional_failures(client, tmp_path, monkeypatch):
    _seed_active_service(monkeypatch, tmp_path)
    monkeypatch.setattr(_views_repo, "get_views", lambda _sid: (_ for _ in ()).throw(RuntimeError("boom")))
    with (
        patch("backend.utils.tunnel.get_tunnel_manager", side_effect=RuntimeError("boom")),
        patch("backend.core.metadata.usage_log.get_cron_runs", side_effect=RuntimeError("boom")),
        patch("backend.core.metadata.usage_log.latest_cron_per_task", side_effect=RuntimeError("boom")),
        patch("backend.scoring.list_labels", side_effect=RuntimeError("boom")),
    ):
        resp = _get(client)
    assert resp.status_code == 200
    data = resp.json()
    for k in ("share_banner", "cron_runs_first_page", "last_sync", "scoring_labels"):
        assert data.get(k) is None
    assert data["views"] == [] and data["active_service_id"] == MOCK_SERVICE_ID
    assert {"services", "pops", "settings"} <= set(data)


# ── Merged from test_pricing_defaults.py ──

_MOCK_GLOBAL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.006,
    "class_b_rate_per_10k": 0.02,
    "cdn_egress_rate_per_gb": 0.15,
    "storage_rate_per_gb_month": 0.03,
}
_TEST_SOURCE = {"name": "test_service", "service_id": "svc1"}


@patch("backend.config.load_usage_logging_config", return_value=_MOCK_GLOBAL_CFG)
def test_prefill_uses_global_rates(mock_load):
    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get("/api/usage/prefill")
    app.dependency_overrides.pop(get_source, None)
    assert r.status_code == 200
    data = r.json()
    for k in ("class_a_rate_per_1k", "class_b_rate_per_10k", "cdn_egress_rate_per_gb", "storage_rate_per_gb_month"):
        assert data[k] == _MOCK_GLOBAL_CFG[k]


@patch("backend.config.load_usage_logging_config", return_value=_MOCK_GLOBAL_CFG)
@patch("backend.core.metadata.get_usage_logs")
def test_usage_log_uses_global_rates_for_cost(mock_get_logs, mock_load):
    mock_get_logs.return_value = (
        [],
        0,
        {
            "total_class_a": 1000,
            "total_class_b": 10000,
            "total_cdn_downloads": 0,
            "total_cdn_bytes": 1073741824,
            "total_fos_bytes": 0,
            "class_a_breakdown": {},
            "class_b_breakdown": {},
        },
    )
    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get("/api/admin/usage-log", params={"start": "2024-01-01 00:00:00", "end": "2024-12-31 23:59:59"})
    app.dependency_overrides.pop(get_source, None)
    assert r.status_code == 200
    agg = r.json()["aggregate"]
    for k, exp in (
        ("estimated_cost_class_a", 0.006),
        ("estimated_cost_class_b", 0.02),
        ("estimated_cost_cdn", 0.15),
        ("estimated_cost_total", 0.176),
    ):
        assert abs(agg[k] - exp) < 1e-10


# ── Merged from tests/routers/test_control_room.py ──

ALL_TABS = ["overview", "performance", "origin", "security", "network", "sessions", "insights", "admin_health"]
LIVE_TABS = ["overview"]
STUB_TABS = [t for t in ALL_TABS if t not in LIVE_TABS]
MUTATION_ENDPOINTS = ["mitigations", "rules", "allowlist", "big-red-button", "cost-governor"]


def _make_cr_client(in_memory_duckdb, test_service_source, admin_fn):
    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]
    app.dependency_overrides[require_service_access] = lambda: MOCK_SERVICE_ID
    app.dependency_overrides[require_admin] = admin_fn
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def admin_client(in_memory_duckdb, test_service_source):
    yield from _make_cr_client(in_memory_duckdb, test_service_source, lambda: None)


@pytest.fixture
def analyst_client(in_memory_duckdb, test_service_source):
    def _reject():
        raise HTTPException(
            status_code=403, detail={"error": "admin_only", "message": "This operation requires admin access."}
        )

    yield from _make_cr_client(in_memory_duckdb, test_service_source, _reject)


@pytest.mark.parametrize("tab", STUB_TABS)
def test_tab_get_admin_stub_200(admin_client, tab):
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}", headers=_SVC_HDR)
    assert resp.status_code == 200 and resp.json()["tab"] == tab and resp.json()["status"] == "stub"


@pytest.mark.parametrize("tab", LIVE_TABS)
def test_tab_get_admin_live_200(admin_client, tab):
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}", headers=_SVC_HDR)
    assert resp.status_code == 200 and resp.json()["tab"] == tab and resp.json()["status"] == "live"


@pytest.mark.parametrize("tab", ALL_TABS)
def test_tab_get_analyst_200(analyst_client, tab):
    resp = analyst_client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}", headers=_SVC_HDR)
    assert resp.status_code == 200 and resp.json()["tab"] == tab


def test_unknown_tab_404(admin_client):
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/bogus", headers=_SVC_HDR)
    assert resp.status_code == 404 and resp.json()["detail"]["error"] == "unknown_tab"


def test_error_stream_route_is_registered():
    assert "/api/services/{service_id}/control-room/error-stream" in app.openapi()["paths"]


def test_error_stream_not_shadowed_by_tab_route(monkeypatch):
    route_paths = [getattr(r, "path", "") for r in cr_mod.router.routes]
    err_idx = route_paths.index("/api/services/{service_id}/control-room/error-stream")
    tab_idx = route_paths.index("/api/services/{service_id}/control-room/{tab}")
    assert err_idx < tab_idx, "error-stream route must be registered before /{tab} catch-all"


def test_correlate_top_values_windowed_freshness_and_telemetry(monkeypatch):
    recorded: list[dict] = []
    monkeypatch.setattr(usage_log, "log_telemetry_query", lambda **kwargs: recorded.append(kwargs))
    executed_sql: list[str] = []

    class _MockResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

    class _MockCon:
        def execute(self, sql, params=None):
            executed_sql.append(sql)
            rows = [("/home", 42)] if "GROUP BY" in sql else [(datetime(2026, 10, 20, 12, 0, tzinfo=UTC),)]
            return _MockResult(rows)

    set_page_load_id("cr-telemetry-test")
    try:
        rows, latest_ts = cr_mod.correlate_top_values(
            _MockCon(), "logs_test", "url", datetime(2026, 10, 20, 11, 0, tzinfo=UTC), 10, service_id="svc_cr"
        )
    finally:
        set_page_load_id(None)

    assert rows == [("/home", 42)] and latest_ts is not None
    assert len(executed_sql) == 2 and "WHERE timestamp >= ?" in executed_sql[1]
    assert len(recorded) == 2 and all(
        r["engine"] == "duckdb" and r["page_load_id"] == "cr-telemetry-test" for r in recorded
    )


def test_recent_standard_errors_maps_rows(monkeypatch):
    row = ("event-1", datetime(2026, 7, 7, tzinfo=UTC), "404", "/missing", "DEN", "192.0.2.10")

    class _Connection:
        def execute(self, query, params=None):
            assert "FROM logs_test_service" in query
            return SimpleNamespace(fetchall=lambda: [row])

        def close(self):
            pass

    monkeypatch.setattr(duckdb, "get_connection", lambda **kwargs: _Connection())
    events = cr_mod._recent_standard_errors({"service_id": MOCK_SERVICE_ID}, "test-service")
    exp = {"event_id": "event-1", "timestamp": "2026-07-07T00:00:00+00:00", "status": 404}
    exp |= {"url": "/missing", "pop": "DEN", "client_ip": "192.0.2.10"}
    assert events == [exp]


def test_recent_high_scale_errors_maps_rows():
    class _Client:
        def execute(self, query, params):
            assert "request_facts" in query and params == {"service_id": MOCK_SERVICE_ID}
            row = {"event_id": "event-2", "timestamp": datetime(2026, 7, 7, tzinfo=UTC), "status": 503}
            return [row | {"url": "/upstream", "pop": "IAD", "client_ip": "198.51.100.7"}]

    events = cr_mod._recent_high_scale_errors(SimpleNamespace(service_id=MOCK_SERVICE_ID, client=_Client()))
    assert events[0]["status"] == 503 and events[0]["client_ip"] == "198.51.100.7"


def test_error_events_mask_only_client_ip(monkeypatch):
    monkeypatch.setattr(cr_mod, "mask_ips_for", lambda session: True)
    monkeypatch.setattr(cr_mod, "mask_ip", lambda value: "masked")
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=object()))
    event = {"event_id": "event-1", "client_ip": "192.0.2.10", "url": "/x"}
    assert cr_mod._mask_error_events([event], request) == [{"event_id": "event-1", "client_ip": "masked", "url": "/x"}]


@pytest.mark.parametrize("endpoint", MUTATION_ENDPOINTS)
def test_mutation_admin_501(admin_client, endpoint):
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{endpoint}",
        headers=_SVC_HDR,
        json={"action": "test", "payload": {}},
    )
    assert resp.status_code == 501 and resp.json()["detail"]["error"] == "not_implemented"


@pytest.mark.parametrize("endpoint", MUTATION_ENDPOINTS)
def test_mutation_analyst_403(analyst_client, endpoint):
    resp = analyst_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{endpoint}",
        headers=_SVC_HDR,
        json={"action": "test", "payload": {}},
    )
    assert resp.status_code == 403 and resp.json()["detail"]["error"] == "admin_only"


class _FakeRequest:
    def __init__(self, disconnect_after: int = 999):
        self._count = 0
        self._disconnect_after = disconnect_after
        self.state = SimpleNamespace(analyst_session=None)

    async def is_disconnected(self) -> bool:
        self._count += 1
        return self._count >= self._disconnect_after


async def _drive_sse_with_payload(monkeypatch, payload: dict) -> dict:
    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())
    monkeypatch.setattr(poller_mod.poller, "ensure_polling", lambda sid: None)
    monkeypatch.setattr(rt_publisher, "publisher", test_publisher)

    resp = await cr_mod.realtime_stream(request=_FakeRequest(disconnect_after=10), service_id=MOCK_SERVICE_ID)
    agen = resp.body_iterator

    async def _delayed_publish():
        await asyncio.sleep(0.05)
        test_publisher.publish(MOCK_SERVICE_ID, payload)

    asyncio.ensure_future(_delayed_publish())
    try:
        return json.loads(await asyncio.wait_for(agen.__anext__(), timeout=5))
    finally:
        await agen.aclose()


@pytest.mark.asyncio
async def test_sse_real_metrics_shape(monkeypatch):
    data = {"requests_per_second": 42.5, "error_rate": 0.01, "cache_hit_ratio": 0.95, "bandwidth_mbps": 1.5}
    data |= {"status_breakdown": {"status_2xx": 100}, "estimated_cost_usd": 0.001, "origin_requests_per_second": 5.0}
    data |= {"origin_bandwidth_mbps": 0.2, "shield_requests": 10, "shield_hit_ratio": 0.05, "pass_requests": 0}
    data |= {
        "synth_requests": 0,
        "waf_blocked": 0,
        "waf_logged": 0,
        "waf_passed": 42,
        "pop_count": 2,
        "degraded_pops": [],
    }
    payload = {"event": "metrics_tick", "event_schema_version": 2, "timestamp": "2026-07-07T00:00:00+00:00"}
    payload |= {"status": "ok", "data": data, "aggregate_delay": 3}
    frame = await _drive_sse_with_payload(monkeypatch, payload)
    assert frame["event"] == "metrics_tick" and frame["event_schema_version"] == 2 and frame["status"] == "ok"
    assert frame["data"]["requests_per_second"] == 42.5
    assert {"cache_hit_ratio", "bandwidth_mbps", "estimated_cost_usd"} <= set(frame["data"])


@pytest.mark.asyncio
async def test_sse_rt_down_payload(monkeypatch):
    frame = await _drive_sse_with_payload(monkeypatch, error_tick_payload())
    assert frame["status"] == "rt_down" and frame["data"]["requests_per_second"] == 0


def test_log_field_audit_200(admin_client, monkeypatch):
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"log_fields": None})
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/log-field-audit", headers=_SVC_HDR)
    assert resp.status_code == 200
    data = resp.json()
    assert data["service_id"] == MOCK_SERVICE_ID
    assert isinstance(data["enabled_fields"], list) and isinstance(data["features"], list) and len(data["features"]) > 0
    for feature in data["features"]:
        assert "name" in feature and feature.get("status") in ("ok", "missing_fields")


def test_log_field_audit_missing_fields(admin_client, monkeypatch):
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"log_fields": {"groups": [], "field_overrides": {}}})
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/log-field-audit", headers=_SVC_HDR)
    assert resp.status_code == 200
    assert any(f["status"] == "missing_fields" for f in resp.json()["features"])


def test_correlate_unknown_dimension_400(admin_client):
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers=_SVC_HDR,
        json={"dimension": "totally_bogus_field"},
    )
    assert resp.status_code == 400 and resp.json()["detail"]["error"] == "unknown_dimension"


def test_openapi_has_control_room_endpoints():
    paths = app.openapi()["paths"]
    for p in (
        "control-room/{tab}",
        "realtime-stream",
        "log-field-audit",
        "control-room/correlate",
        "control-room/mitigations",
    ):
        assert f"/api/services/{{service_id}}/{p}" in paths


def test_realtime_seed_without_credentials_skips_network(monkeypatch):
    monkeypatch.setattr(svcconfig, "get_fastly_api_key", lambda service_id: None)
    monkeypatch.setattr(svcconfig, "get_fastly_logging_service_id", lambda service_id: None)

    def _fail_get(*args, **kwargs):
        raise AssertionError("seed fetch with no credentials attempted network I/O")

    monkeypatch.setattr("requests.get", _fail_get)
    monkeypatch.setattr("backend.core.fastly.client.fastly_rt", _fail_get)
    assert cr_mod._fetch_seed_ticks(MOCK_SERVICE_ID, count=5) == []


def test_fetch_seed_ticks_goes_through_fastly_rt_client(monkeypatch):
    monkeypatch.setattr(svcconfig, "get_fastly_api_key", lambda service_id: "tok")
    monkeypatch.setattr(svcconfig, "get_fastly_service_id", lambda service_id: "cdn123")
    calls = []

    def _fake_rt(path, *, token, **kwargs):
        calls.append((path, token))
        return {"Timestamp": 1000, "Data": [{"aggregated": {}}]}

    monkeypatch.setattr("backend.core.fastly.client.fastly_rt", _fake_rt)
    monkeypatch.setattr("backend.core.realtime.transform_single_second", lambda point, ts: {"ts": ts})
    ticks = cr_mod._fetch_seed_ticks(MOCK_SERVICE_ID, count=5)
    assert calls == [("/v1/channel/cdn123/ts/h?limit=120", "tok")] and len(ticks) == 1


def test_fetch_seed_ticks_returns_empty_when_rt_fetch_fails(monkeypatch):
    monkeypatch.setattr(svcconfig, "get_fastly_api_key", lambda service_id: "tok")
    monkeypatch.setattr(svcconfig, "get_fastly_service_id", lambda service_id: "cdn123")

    def _boom(path, *, token, **kwargs):
        raise RuntimeError("HTTP 500 GET /v1/channel/cdn123/ts/h")

    monkeypatch.setattr("backend.core.fastly.client.fastly_rt", _boom)
    assert cr_mod._fetch_seed_ticks(MOCK_SERVICE_ID, count=5) == []


def test_realtime_seed_with_cached_ticks(admin_client, monkeypatch):
    monkeypatch.setattr(
        rt_publisher, "get_recent_ticks", lambda sid, count: [{"timestamp": "2026-07-07T00:00:00Z"}] * 60
    )
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/realtime-seed", headers=_SVC_HDR)
    assert resp.status_code == 200 and len(resp.json()["ticks"]) == 60


def test_realtime_seed_with_fetch_fallback(admin_client, monkeypatch):
    monkeypatch.setattr(rt_publisher, "get_recent_ticks", lambda sid, count: [])
    monkeypatch.setattr(cr_mod, "_fetch_seed_ticks", lambda sid: [{"timestamp": "2026-07-07T00:01:00Z"}])
    monkeypatch.setattr(poller_mod.poller, "ensure_polling", lambda sid: None)
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/realtime-seed", headers=_SVC_HDR)
    assert resp.status_code == 200 and resp.json()["ticks"] == [{"timestamp": "2026-07-07T00:01:00Z"}]


def test_correlate_success(admin_client, monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": "svc"})
    mock_con = MagicMock()
    mock_con.execute().fetchall.return_value = [("200", 100)]
    mock_con.execute().fetchone.return_value = (datetime(2026, 8, 19, 12, 0, 0, tzinfo=UTC),)
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda source, read_only: mock_con)

    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers=_SVC_HDR,
        json={"dimension": "status", "window_minutes": 10, "limit": 5},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["dimension"] == "status" and data["top"] == [{"value": "200", "count": 100}] and "freshness" in data


@pytest.mark.asyncio
async def test_correlate_masks_client_ips_for_analyst(monkeypatch):
    mock_con = MagicMock()
    mock_con.execute().fetchall.return_value = [("203.0.113.45", 7)]
    mock_con.execute().fetchone.return_value = (datetime.now(UTC),)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": "svc"})
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda source, read_only: mock_con)

    request = SimpleNamespace(state=SimpleNamespace(analyst_session=SimpleNamespace(pii_policy={"mask_ips": True})))
    response = await cr_mod.control_room_correlate(
        cr_mod.CorrelateRequest(dimension="ip"),
        MOCK_SERVICE_ID,
        request,
        None,
        SimpleNamespace(resolve=lambda service_id: None),
    )
    assert response["top"] == [{"value": "203.0.113.xxx", "count": 7}]


@pytest.mark.asyncio
async def test_correlate_uses_high_scale_aggregate_path():
    watermark = ServingWatermark(
        service_id=MOCK_SERVICE_ID,
        domain="request",
        owner_epoch=1,
        coverage_start=datetime(2026, 8, 19, 12, 0, tzinfo=UTC),
        coverage_end=datetime(2026, 8, 19, 12, 5, tzinfo=UTC),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=None,
        exact=True,
    )

    class Client:
        def execute(self, sql, params=None):
            return [{"total_count": 12}] if "AS total_count" in sql else [{"value": "500", "aggregate_count": 12}]

    service = SimpleNamespace(client=Client(), watermark_for=lambda domain: watermark)
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=None))
    response = await cr_mod.control_room_correlate(
        cr_mod.CorrelateRequest(dimension="status"),
        MOCK_SERVICE_ID,
        request,
        None,
        SimpleNamespace(resolve=lambda service_id: service),
    )
    assert response["top"] == [{"value": "500", "count": 12}]
    assert response["freshness"]["latest_log_at"] == "2026-08-19T12:05:00+00:00"

    waf_sig_response = await cr_mod.control_room_correlate(
        cr_mod.CorrelateRequest(dimension="waf_sig"),
        MOCK_SERVICE_ID,
        request,
        None,
        SimpleNamespace(resolve=lambda service_id: service),
    )
    assert waf_sig_response["dimension"] == "waf_sig"
    assert waf_sig_response["top"] == [{"value": "500", "count": 12}]


def test_correlate_unknown_service_source_404(admin_client, monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate", headers=_SVC_HDR, json={"dimension": "status"}
    )
    assert resp.status_code == 404 and resp.json()["detail"]["error"] == "no_data_source"


def test_wizard_state_and_step_flow(admin_client):
    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/state", headers=_SVC_HDR)
    assert resp.status_code == 200 and resp.json()["completed_steps"] == [] and resp.json()["is_complete"] is False

    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/step",
        headers=_SVC_HDR,
        json={"step": "domain", "details": {"custom": True}},
    )
    assert resp.status_code == 200 and resp.json()["recorded"] is True and resp.json()["step"] == "domain"

    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/step",
        headers=_SVC_HDR,
        json={"step": "complete", "details": {}},
    )
    assert resp.status_code == 200

    resp = admin_client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/state", headers=_SVC_HDR)
    assert resp.status_code == 200 and {"domain", "complete"} <= set(resp.json()["completed_steps"])


# ── Merged from tests/routers/test_rbac_and_typed_contracts.py ──


def _body(resp, *, status: int = 200):
    """Assert HTTP status and return JSON with telemetry underscore keys stripped."""
    assert resp.status_code == status, resp.text
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def _roundtrip(obj: Any) -> Any:
    return json.loads(json.dumps(jsonable_encoder(obj)))


def _make_analyst_session(session_id: str = "sess-analyst-1", **ov: Any) -> AnalystSession:
    d: dict[str, Any] = {"session_id": session_id, "invite_id": "inv-analyst-1", "name": "Analyst B"}
    d |= {"email": "analyst@example.com", "ip_address": "203.0.113.10", "user_agent": "pytest-analyst"}
    d |= {"fingerprint_signature": "fp-analyst", "pii_policy": {"mask_ips": True}, "query_window_hours": 24}
    d |= {"query_start_time": None, "query_end_time": None, "login_time": "2026-01-01T00:00:00Z"}
    d |= {"last_active_time": "2026-01-01T00:05:00Z", "service_ids": ["svc-1"]}
    return AnalystSession(**(d | ov))


# ── Cross-tenant scope enforcement (alerts, views, custom fields, scoring labels) ──


@pytest.fixture
def app_with_session():
    """Mini app that injects a per-test analyst_session on request.state via X-Test-Session-Services."""
    mini = FastAPI()
    for rtr in (
        alerts_router.alerts_router,
        alerts_router.views_router,
        services_core_router.router,
        session_scoring_router.router,
    ):
        mini.include_router(rtr)

    @mini.middleware("http")
    async def stamp_session(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        sids = [s for s in (sid or "").split(",") if s]
        request.state.analyst_session = (
            SimpleNamespace(session_id="test", service_ids=sids, email="t@t") if sid is not None else None
        )
        return await call_next(request)

    return mini


def _fake_alert(aid: str, sid: str) -> dict:
    d = {"id": aid, "service_id": sid, "name": "t", "category": "reliability", "metric": "5xx_rate"}
    d |= {"evaluation_type": "absolute", "evaluation_scope": "all", "operator": ">"}
    return d | {"threshold": 5.0, "window_min": 5.0}


_VALID_CF_BODY = {
    "name": "x_test_field",
    "label": "Test Field",
    "vcl_log_expression": "req.http.X-Test",
    "collection_stage": "deliver",
} | {"duckdb_type": "VARCHAR", "value_type": "string", "bytes_estimate": 20, "enabled": True}


def _scope_403(resp) -> bool:
    return resp.status_code == 403 and resp.json().get("detail", {}).get("error") == "service_not_authorized"


@pytest.mark.security_regression
def test_alerts_list_filters_to_analyst_scope_and_admin_sees_all(app_with_session):
    fake_alerts = [_fake_alert("a1", "svc-A"), _fake_alert("a2", "svc-B"), _fake_alert("a3", "svc-C")]
    with patch("backend.repositories.assets.get_alerts", return_value=fake_alerts), TestClient(app_with_session) as c:
        r = c.get("/api/alerts/", headers={"x-test-session-services": "svc-A,svc-B"})
        assert r.status_code == 200 and [a["id"] for a in r.json()["data"]] == ["a1", "a2"]
        r_admin = c.get("/api/alerts/")
        assert r_admin.status_code == 200 and len(r_admin.json()["data"]) == 3


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path,patch_target",
    [
        ("/api/alerts/svc-A", "backend.repositories.assets.get_alerts"),
        ("/api/views/svc-A", "backend.repositories.assets.get_views"),
    ],
)
def test_alerts_and_views_service_scope_checks(app_with_session, path: str, patch_target: str):
    with TestClient(app_with_session) as c:
        r_deny = c.get(path.replace("svc-A", "svc-B"), headers={"x-test-session-services": "svc-A"})
        assert _scope_403(r_deny), r_deny.text
    with patch(patch_target, return_value=[]), TestClient(app_with_session) as c:
        assert c.get(path, headers={"x-test-session-services": "svc-A"}).status_code == 200


@pytest.mark.security_regression
def test_views_list_admin_unrestricted(app_with_session):
    row = {"id": "v1", "service_id": "svc-anything", "name": "My view", "filters_json": "[]", "time_range_type": None}
    row |= {"start_time": None, "end_time": None, "page": None, "created_at": "2026-01-01 00:00:00"}
    with patch("backend.repositories.assets.get_views", return_value=[row]), TestClient(app_with_session) as c:
        r = c.get("/api/views/svc-anything")
    assert r.status_code == 200 and r.json() == [row]


@pytest.mark.security_regression
def test_alerts_and_views_create_rejects_unauthorized_analyst(app_with_session):
    alert_payload = {k: v for k, v in _fake_alert("a", "svc-B").items() if k != "id"} | {
        "name": "test",
        "window_min": 5,
    }
    view_payload = {"service_id": "svc-B", "name": "v", "filters": {}, "columns": []}
    with TestClient(app_with_session) as c:
        assert (
            c.post("/api/alerts/", json=alert_payload, headers={"x-test-session-services": "svc-A"}).status_code == 403
        )
        r_view = c.post("/api/views/", json=view_payload, headers={"x-test-session-services": "svc-A"})
        assert r_view.status_code in (403, 422) and (r_view.status_code != 403 or _scope_403(r_view))


@pytest.mark.security_regression
@pytest.mark.parametrize("view_exists", [False, True], ids=["nonexistent", "existing"])
def test_views_delete_rejects_unauthorized_analyst(app_with_session, view_exists: bool):
    """Finding 017: scope gate runs before any DB lookup so both existing and non-existent ids return 403."""
    view_row = {"id": "v-target", "service_id": "svc-B", "name": "n"} if view_exists else None
    with patch("backend.repositories.assets.get_view_by_id", return_value=view_row), TestClient(app_with_session) as c:
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
    [("svc-anything", {}), ("svc-A", {"x-test-session-services": "svc-A"})],
    ids=["admin_unrestricted", "in_scope_analyst"],
)
def test_custom_fields_authorized_callers_reach_handler(app_with_session, service_id: str, headers: dict):
    with (
        patch("backend.utils.router_utils.load_service_config", return_value={"log_fields": {"custom_fields": []}}),
        TestClient(app_with_session) as c,
    ):
        assert c.get(f"/api/services/{service_id}/custom-fields", headers=headers).status_code != 403


@pytest.mark.security_regression
def test_scoring_labels_delete_scope_and_admin_bypass(app_with_session):
    with TestClient(app_with_session) as c:
        r_deny = c.delete(
            "/api/services/svc-B/scoring/labels/some-label-id", headers={"x-test-session-services": "svc-A"}
        )
    assert _scope_403(r_deny) and r_deny.json()["detail"]["service"] == "svc-B"
    with (
        patch("backend.scoring.delete_label", return_value={"id": "x", "service_id": "svc-B"}),
        TestClient(app_with_session) as c,
    ):
        assert c.delete("/api/services/svc-B/scoring/labels/x").status_code != 403


# ── Blocklist enforcement (H-1..H-4, S-1, S-2) ───────────────────────────────

_SCORING_ADMIN_SUFFIXES = ("/config", "/status", "/audit", "/threshold", "/exclude-regex", "/enforce-status-code")
_BLOCKED_BASE_PATHS = [
    "/api/usage/prefill",
    "/api/download-all",
    "/api/download-folder",
    "/api/download",
    "/api/cron-schedule",
    "/api/services/svc-A/log-field-audit",
    *(f"/api/services/svc-A/scoring{s}" for s in _SCORING_ADMIN_SUFFIXES),
]


@pytest.fixture
def app_with_blocklist():
    """Mini app whose middleware runs ``_is_blocked_path`` for analyst requests."""
    mini = FastAPI()

    @mini.middleware("http")
    async def rbac_gate(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        if sid is not None:
            request.state.analyst_session = SimpleNamespace(
                session_id="test", service_ids=[s for s in sid.split(",") if s], email="t@t"
            )
            if _is_blocked_path(request.url.path):
                return JSONResponse(status_code=403, content={"error": "admin_only"})
        else:
            request.state.analyst_session = None
        return await call_next(request)

    routes = (
        "/api/usage/prefill",
        "/api/download",
        "/api/download-all",
        "/api/download-folder",
        "/api/cron-schedule",
        "/api/services/{service_id}/realtime-stream",
        "/api/services/{service_id}/realtime-seed",
        "/api/services/{service_id}/control-room/{tab}",
        "/api/services/{service_id}/log-field-audit",
        *(
            f"/api/services/{{service_id}}/scoring{s}"
            for s in (*_SCORING_ADMIN_SUFFIXES, "/labels", "/sessions/{sid}/events", "/top-flagged")
        ),
    )
    for route in routes:
        mini.add_api_route(
            route,
            lambda service_id="svc-A", sid="", tab="": {"ok": True, "service_id": service_id, "sid": sid, "tab": tab},
            methods=["GET"],
        )
    return mini


@pytest.mark.security_regression
@pytest.mark.parametrize("path", [*_BLOCKED_BASE_PATHS, "/api/services/svc-A/scoring/l2-enforce"])
def test_admin_only_routes_blocked_for_analyst(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403 and r.json()["error"] == "admin_only", f"{path}: {r.status_code} {r.text}"


@pytest.mark.security_regression
@pytest.mark.parametrize("path", [f"{p}/" for p in _BLOCKED_BASE_PATHS])
def test_admin_only_routes_resist_trailing_slash_bypass(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403 and r.json()["error"] == "admin_only", f"{path}: {r.status_code} {r.text}"


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
    assert r.status_code == 200 and r.json()["service_id"] == "svc-A", f"{path}: {r.status_code} {r.text}"


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
    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda method, path, **kw: (
            {"id": "tok-1", "name": "T", "user_id": "u"} if "/tokens/self" in path else {"name": "Svc"}
        ),
    ):
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


def _storage_calendar(client):
    from backend.core.request_context import get_source

    app.dependency_overrides[get_source] = lambda: {"service_id": "service123", "name": "service123"}
    with patch(
        "backend.core.storage.get_snapshot_calendar", return_value={"2026-08-19": {"data_files": 5, "size_bytes": 1}}
    ):
        return client.get("/api/admin/storage-calendar?table_name=logs")


def _log_fields_get(client):
    svcconfig.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "log_fields": {"groups": ["A"]}})
    return client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


_SITES = [
    pytest.param(_validate, id="provision-validate"),
    pytest.param(_check_domain, id="provision-check-domain"),
    pytest.param(lambda c: _check_fos(c, fail=False), id="provision-check-fos-ok"),
    pytest.param(lambda c: _check_fos(c, fail=True), id="provision-check-fos-failed"),
    pytest.param(_audit_logs, id="audit-logs"),
    pytest.param(_storage_calendar, id="storage-calendar"),
    pytest.param(_log_fields_get, id="service-log-fields"),
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
        statement_profiler.clear()
        monkeypatch.setattr(starlette.datastructures.State, "__getattr__", _mock_state_getattr)
        yield
        _is_remote_flag = False
        statement_profiler.clear()

    def test_recent_sqlite_lifecycle_and_filters(self):
        client = TestClient(app)
        empty = client.get("/api/debug/recent-sqlite").json()
        assert empty["queries"] == [] and empty["buffer_size"] == 0 and empty["buffer_cap"] > 0

        metadata_db.get_con("svc-a").execute("SELECT 1")
        first = client.get("/api/debug/recent-sqlite").json()
        assert first["buffer_size"] > 0 and any("SELECT 1" in q["sql"] for q in first["queries"])

        midpoint_seq = first["last_seq"]
        con2 = metadata_db.get_con("svc-b")
        con2.execute("SELECT 2")
        con2.execute("SELECT 3")
        second = client.get(f"/api/debug/recent-sqlite?since_seq={midpoint_seq}").json()
        assert all(q["seq"] > midpoint_seq for q in second["queries"])
        assert len(client.get("/api/debug/recent-sqlite?limit=2").json()["queries"]) <= 2

        clear_res = client.post("/api/debug/clear-sqlite")
        assert clear_res.status_code == 200 and clear_res.json()["ok"] is True
        assert statement_profiler.get_recent()["buffer_size"] == 0

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
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False) if env_val is None else monkeypatch.setenv(
            "DEBUG_RESPONSES", env_val
        )
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
        assert res_blocked.status_code == 403 and res_blocked.json()["detail"]["error"] == "sql_debug_disabled"

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


def test_admin_bot_sources_rdns_and_duckdb_match_producer(client):
    meta = {"id": "arcjet", "entry_count": 42, "last_updated": "2026-10-11T00:00:00Z", "enabled": True}
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=meta):
        assert _body(client.post("/api/admin/bot-sources/arcjet/refresh")) == {"ok": True, "source": meta}
    results = [{"id": "a", "entry_count": 3}, {"id": "b", "failed": True, "error": "boom"}]
    for path in ("/api/admin/bot-sources/refresh", "/api/admin/bots/refresh"):
        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=results):
            assert _body(client.post(path)) == {"ok": False, "sources": results, "updated_count": 1, "failed_count": 1}
    summary = {"resolved": 4, "errors": 1, "discovered": 7, "batch_limit": 100}
    with patch("backend.utils.bot_sources.enrich_batch", return_value=summary) as enrich:
        assert _body(client.post("/api/admin/rdns/enrich?limit=100")) == {"ok": True, "summary": summary}
    enrich.assert_called_once_with(limit=100)

    mb = 1024 * 1024
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * mb),
        patch("backend.cron.jobs.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
    ):
        assert _body(client.post("/api/admin/duckdb/recycle")) == dict(
            ok=True, status="skipped", reason="RSS 100MB < threshold 500MB", rss_mb=100.0, threshold_mb=500.0
        )
    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * mb, 200 * mb]),
        patch("backend.cron.jobs.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
        patch("backend.cron.jobs.duckdb_recycle.recycle_once", return_value="manual_admin: recycled 1/1"),
    ):
        exp_recycled = {"ok": True, "status": "recycled", "detail": "manual_admin: recycled 1/1"}
        exp_recycled |= {"rss_before_mb": 300.0, "rss_after_mb": 200.0, "freed_mb": 100.0}
        assert _body(client.post("/api/admin/duckdb/recycle?force=true")) == exp_recycled
    status_body = _body(client.get("/api/admin/duckdb/status"))
    assert set(status_body) == {"ok", "memory", "barrier", "pools", "retired_pools"}
    assert set(status_body["memory"]) == {"current_rss_mb", "recycle_threshold_mb", "recycle_interval_min"}
    assert set(status_body["barrier"]) == {"is_active", "active_paths"}


def test_ngwaf_share_purge_calendar_and_telemetry_match_producer(client, monkeypatch):
    stats = {"total_cached_bots": 2, "workspaces": {"w": None}, "top_bots": {"bot": 2}}
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-n"}),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync"),
        patch("backend.core.rollups.security_dims.get_cache_stats", return_value=stats),
    ):
        exp_sync = {"ok": True, "service_id": "svc-n", "workspace_id": "ws-1", "stats": stats}
        assert _body(client.post("/api/admin/ngwaf/sync/svc-n")) == exp_sync
    monkeypatch.setattr("backend.core.rollups.security_dims.get_cache_stats", lambda: dict(stats))
    assert _body(client.get("/api/admin/ngwaf/status")) == {"ok": True, **stats}
    monkeypatch.setattr("backend.utils.bot_sources.get_stats", lambda: {"entries": 4, "hit_rate": 0.5})
    assert _body(client.get("/api/admin/rdns/stats")) == {"ok": True, "stats": {"entries": 4, "hit_rate": 0.5}}
    purge_stats = {"deleted_audit_logs": 3, "deleted_expired_invites": 1, "deleted_stale_sessions": 2}
    purge_stats |= {"deleted_claim_tokens": 0, "audit_retention_days": 90, "max_idle_session_days": 30}
    with patch("backend.core.share_db.purge_all_share_records", return_value=purge_stats):
        assert _body(client.post("/api/admin/share/purge")) == {"ok": True, **purge_stats}
    calendar = {"2026-10-01": {"data_files": 4, "size_bytes": 0}, "2026-10-02": {"data_files": 9, "size_bytes": 0}}
    with patch("backend.core.storage.get_snapshot_calendar", return_value=calendar):
        assert _body(client.get("/api/admin/storage-calendar")) == calendar

    call_row = {
        "service": "FOS",
        "method": "GET",
        "path": "/b/k",
        "status": "200",
        "time_ms": 3.0,
        "caller": "fn",
        "bytes": 10,
    }
    payload = {
        "queries": [{"engine": "duckdb", "sql_query": "SELECT 1", "duration_ms": 1.5}],
        "sections": [{"section_name": "summary", "duration_ms": 2.0}],
        "calls": [call_row],
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


def test_admin_ingest_actions_beacons_celery_and_control_room_match_producer(client, monkeypatch):
    started = {"ok": True, "message": "started", "run_id": 41}
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value=started):
        assert _body(client.post("/api/admin/commit-storage"), status=202) == started
        assert _body(client.post("/api/admin/ingest-logs")) == started
    with patch("backend.core.storage.clear_source_caches"), patch("threading.Thread"):
        assert _body(client.post("/api/admin/rebuild-local-view"), status=202) == {
            "ok": True,
            "message": "Local view rebuild started.",
        }
    cfg = {"service_id": "svc-t", "remote_frontend": {"service_id": "remote-1"}}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=cfg),
        patch("backend.routers.share_admin.delete_remote_frontend"),
        patch("backend.config.mutate_config", return_value=cfg),
    ):
        assert _body(
            client.post("/api/sharing/teardown-frontend", json={"service_id": "svc-t", "token_override": "t"})
        ) == {"ok": True, "message": "Successfully deleted remote frontend service remote-1"}
    assert _body(client.post("/api/ux-events", json={"event": "column_reordered", "pathname": "/x"})) == {"ok": True}
    assert _body(client.post("/api/web-vitals", json={"id": "v1", "name": "LCP", "value": 1.2, "rating": "good"})) == {
        "ok": True
    }

    for payload in (
        dict(
            broker_reachable=True,
            queues={"q.ingest": 3},
            ledger={"committed": 5},
            inprocess_schedules=[{"name": "job", "trigger": "interval[0:01:00]", "next_run_at": 1700000000.0}],
            workers=dict(
                active_tasks={"w1": [{"id": "t1", "name": "n", "args": [1], "time_start": 2.5, "worker_pid": 7}]},
                stats={"w1": {"started": dt.datetime(2026, 1, 1, tzinfo=dt.UTC)}},
                registered={"w1": ["a.b"]},
                scheduled={},
                count=1,
            ),
        ),
        dict(
            broker_reachable=False,
            queues={},
            inprocess_schedules=[],
            ledger={},
            workers={"active_tasks": {}, "stats": {}, "registered": {}, "scheduled": {}, "count": 0, "error": "down"},
        ),
    ):
        monkeypatch.setattr("backend.routers.admin.health.get_celery_status", lambda p=payload: p)
        assert _body(client.get("/api/admin/celery/status")) == _roundtrip(payload)
    for tab, stub in TAB_STUBS.items():
        assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}")) == stub, tab


@pytest.mark.parametrize(
    "model_cls,payload",
    [
        (m_svc.RealtimeSeedResponse, {"ticks": [{"ts": 1, "rps": 2.5, "nested": {"a": [1]}}]}),
        (
            m_svc.LogFieldAuditResponse,
            {
                "service_id": "svc",
                "enabled_fields": ["ip", "url"],
                "features": [{"name": "Insights", "required_fields": ["ip"], "status": "ok", "missing_fields": []}],
            },
        ),
        (
            m_svc.CorrelateResponse,
            {
                "dimension": "country",
                "top": [{"value": "US", "count": 3}],
                "freshness": {"latest_log_at": None, "lag_seconds": 0},
            },
        ),
        (m_svc.WizardStateResponse, {"service_id": "svc", "completed_steps": ["a"], "is_complete": False}),
        (m_svc.WizardStepResponse, {"recorded": True, "step": "a"}),
        (m_scoring.ScoringEnforceThresholdResponse, {"threshold": None, "enforced": False, "key": "enforce_threshold"}),
        (m_scoring.ScoringEnforceThresholdResponse, {"threshold": 70, "enforced": True, "key": "enforce_threshold"}),
        (m_scoring.ScoringDistributionRow, {"hour": "2026-01-01T00:00:00", "bucket": "0-25", "count": 4}),
        (m_scoring.ScoringComplianceRow, {"hour": "2026-01-01T00:00:00", "compliance": "ok", "count": 4}),
        (
            m_scoring.ScoringTopFlaggedRow,
            {"timestamp": "2026-01-01T00:00:00", "edge_sid": None, "edge_score": 90, "url": "/a"},
        ),
        (m_common.BootstrapPopGeo, {"city": "Denver", "region": "co", "country": "us"}),
        (m_common.BootstrapPopGeo, {"city": "", "region": "", "country": ""}),
        (
            m_common.BootstrapDebugState,
            {
                "debug_responses_enabled": False,
                "query_debug_visibility": "disabled",
                "api_call_debug_visibility": "admins",
            },
        ),
        (
            m_prov.ProvisionCheckConfigResponse,
            {"logging_service": {"ok": True, "details": "ok"}, "cdn_service": {"ok": False, "details": ""}},
        ),
        (
            m_prov.NgwafWorkspacesResponse,
            {
                "workspaces": [{"id": "w", "name": "n"}],
                "error_hint": "h",
                "_debug_raw": "GET /x\n\n--- Response (200) ---",
            },
        ),
        (
            m_prov.ProvisionValidateResponse,
            {"service_name": "s", "token_info": {"id": "1", "name": "t", "user_id": None, "type": "automation"}},
        ),
        (m_sec.SecurityBotRow, {"id": "gptbot", "name": "Gptbot", "category": "ai", "request_count": 12}),
        (m_sec.SecurityBotRow, {"name": "Googlebot", "category": "search", "request_count": 5}),
        (
            m_sec.SecurityBotRow,
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
    _assert_same_bytes(m_scoring.ScoringL2EnforceState, scoring_admin._l2_enforce_unavailable())
    for kwargs in (
        {
            "scoring_enabled_at_raw": "1700000000",
            "l2_enforce_raw": "1",
            "l2_enabled_at_raw": "1700086400",
            "now_epoch": 1700200000,
        },
        {"scoring_enabled_at_raw": None, "l2_enforce_raw": None, "l2_enabled_at_raw": None, "now_epoch": 1700200000},
    ):
        _assert_same_bytes(m_scoring.ScoringL2EnforceState, scoring_admin._build_l2_enforce_block(**kwargs))

    scoring_cfg = {"enabled": True, "operator_threshold": 80, "operator_threshold_set_at": "2026-01-01T00:00:00+00:00"}
    scoring_cfg |= {"exclude_url_regex": "\\.(png|css)$", "enforce_status_code": 403}
    cfg: dict[str, Any] = {"service_id": "svc", "scoring": scoring_cfg}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    for model_cls, fn in (
        (m_scoring.ScoringThresholdState, scoring_admin.scoring_threshold_get),
        (m_scoring.ScoringExcludeRegexState, scoring_admin.scoring_exclude_regex_get),
        (m_scoring.ScoringEnforceStatusCodeState, scoring_admin.scoring_enforce_status_code_get),
    ):
        _assert_same_bytes(model_cls, fn("svc"))
    cfg["scoring"] = {"enabled": True}
    _assert_same_bytes(m_scoring.ScoringThresholdState, scoring_admin.scoring_threshold_get("svc"))
    _assert_same_bytes(m_scoring.ScoringEnforceStatusCodeState, scoring_admin.scoring_enforce_status_code_get("svc"))

    for model_cls, invalid in (
        (m_scoring.ScoringTopFlaggedRow, {"edge_sid": "x", "edge_score": 1}),
        (m_prov.ProvisionCheckConfigResponse, {"logging_service": {"ok": True, "details": "x"}}),
        (m_scoring.ScoringLabelRow, {"id": "a", "sid": "s", "label": "maybe"}),
    ):
        with pytest.raises(ValidationError):
            model_cls.model_validate(invalid)

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
    assert m_scoring.ScoringLabelDeleteResponse.model_validate({"status": "success", "id": "abc"}).model_dump(
        mode="json"
    ) == {"status": "success", "id": "abc"}

    ident_payload = {"enabled": True, "scoring_service_id": "svc", "scoring_service_name": "n"}
    ident_payload |= {
        "scoring_domain": "d.example",
        "matrix_version": "2026-01-01",
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }
    assert set(ident_payload) <= set(m_scoring.ScoringStatusResponse.model_fields)
    assert (
        m_scoring.ScoringStatusResponse.model_validate(ident_payload).model_dump(mode="json", exclude_unset=True)
        == ident_payload
    )

    base_row = {"id": "a1", "service_id": MOCK_SERVICE_ID, "sid": "s1", "notes": "n", "flagged_by": "admin"}
    base_row |= {
        "sample_ip": "",
        "sample_ua": "",
        "sample_url": "",
        "created_at": "2026-01-01 00:00:00",
        "updated_at": "2026-01-01 00:00:00",
    }
    rows = [{**base_row, "label": lbl} for lbl in ("good", "bad", "neutral")]
    counts = {"good": 1, "bad": 1, "neutral": 1}
    monkeypatch.setattr(_labels, "list_labels", lambda sid, limit=500: rows)
    monkeypatch.setattr(_labels, "counts_by_label", lambda sid: counts)
    lbl_resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/labels")
    assert _body(lbl_resp) == {"labels": rows, "counts": counts} and list(lbl_resp.json()["counts"]) == [
        "good",
        "bad",
        "neutral",
    ]
    assert m_scoring.ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "good"}).label == "good"
    assert set(typing.get_args(m_scoring.ScoringLabelRow.model_fields["label"].annotation)) == set(ALLOWED_LABELS)


def test_catalog_bootstrap_schema_audit_cmcd_and_query_contracts(client, monkeypatch):
    payload = _compute_log_fields_catalog(None)
    assert payload["groups"] and payload["fields"] and payload["insights"] and payload["presets"]
    _assert_same_bytes(m_common.LogFieldsCatalogResponse, payload)
    assert _body(client.get("/api/log-fields/catalog")) == _roundtrip(payload)
    assert m_common.BootstrapResponse(active_service_id=None, services=[], log_fields_catalog=payload).model_dump(
        mode="json", by_alias=True
    )["log_fields_catalog"] == _roundtrip(payload)

    entries = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_field", "label": "My Field", "description": "d", "enabled": True}]}
    )
    assert entries and all(
        json.loads(_wire_model(m_common.LogFieldCatalogEntry, e)) == jsonable_encoder(e) for e in entries
    )
    custom_payload = {"groups": [], "fields": entries, "insights": [], "presets": {}}
    dumped_cat = m_common.LogFieldsCatalogResponse.model_validate(custom_payload).model_dump(mode="json")
    assert dumped_cat == _roundtrip(custom_payload) and "typical_bytes" not in dumped_cat["fields"][0]

    assert _body(client.get("/api/debug/state")) == {"debug_responses_enabled": m_common._debug_responses_enabled()}
    schema_payload = {"schema": [{"name": "ip", "type": "VARCHAR"}], "table_name": "logs_x"}
    assert m_svc.SchemaResponse.model_validate(schema_payload).model_dump(mode="json", by_alias=True) == schema_payload

    entry = {"id": 7, "timestamp": "2026-01-01 00:00:00+00:00", "event_type": "provision"}
    entry |= {"details": {"bucket": "b", "log_fields": {"groups": ["A"]}}, "actor": "ui", "source": "local"}
    dumped_audit = m_svc.AuditLogsResponse.model_validate(
        {"total": 1, "page": 1, "per_page": 50, "entries": [entry], "_debug_calls": []}
    ).model_dump(mode="json")
    assert dumped_audit["entries"] == [entry] and list(dumped_audit["entries"][0]) == list(entry)

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")) == {"enabled": False}
    cmcd_cfg = {"cmcd": {"enabled": True, "mode": "headers", "version": 1, "enabled_at": "2026-01-01T00:00:00+00:00"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cmcd_cfg)
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")) == cmcd_cfg["cmcd"]

    row_d = {"ts": dt.datetime(2026, 1, 2, 3, 4, 5), "tz_ts": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC)}
    row_d |= {"amount": Decimal("12.50"), "n": 7}
    base_result = {"columns": ["ts", "tz_ts", "amount", "n"], "data": [row_d], "row_count": 1, "total_rows": 1}
    base_result |= {
        "truncated": False,
        "elapsed_ms": 3,
        "explain_plan": "plan",
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }
    with patch("backend.repositories.query.execute_query", return_value=dict(base_result)):
        body = _body(client.post("/api/query", headers=_SVC_HDR, json={"sql": "SELECT 1"}))
    assert (
        body["data"] == _roundtrip(base_result["data"])
        and body["data"][0]["tz_ts"].endswith("+00:00")
        and body["data"][0]["amount"] == 12.5
    )
    with patch(
        "backend.repositories.query.execute_query",
        return_value={k: v for k, v in base_result.items() if k != "explain_plan"},
    ):
        assert "explain_plan" not in _body(client.post("/api/query", headers=_SVC_HDR, json={"sql": "SELECT 1"}))


_RUM_EMPTY_VITALS = {
    "lcp": {"p75": None, "distribution": None},
    "cls": {"p75": None, "distribution": None},
    "inp": {"p75": None, "distribution": None},
    "fid": {"p75": None, "fcp": None, "ttfb": None},
}
_RUM_FULL_VITALS = {
    "lcp": {"p75": 2.5, "distribution": {"good": 80, "needs_improvement": 15, "poor": 5}},
    "cls": {"p75": 0.1, "distribution": {"good": 90, "needs_improvement": 10, "poor": 0}},
    "inp": {"p75": 180, "distribution": {"good": 70, "needs_improvement": 20, "poor": 10}},
    "fid": {"p75": 3.0, "fcp": 1.2, "ttfb": 0.4},
}


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
            }
            | {
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
            }
            | {
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
            }
            | {"message": "Waiting", "vitals": _RUM_EMPTY_VITALS, "worst_pages": [], "errors": []}
            | {
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
                "vitals": _RUM_FULL_VITALS,
            }
            | {
                "worst_pages": [
                    {"path": "/a", "views": 4, "avg_load_time": 1, "lcp_p75": None, "cls_p75": 0.05, "error_rate": 0.0}
                ]
            }
            | {"errors": [{"message": "boom", "file": "a.js", "line": 3, "col": 4, "count": 2}]}
            | {
                "trends": {
                    "timestamps": ["2026-01-01T00:00:00+00:00"],
                    "lcp": [None],
                    "cls": [0.1],
                    "error_rate": [0.0],
                    "pageviews": [10],
                    "interactions": [1],
                    "errors": [1],
                }
            }
            | {"environments": {"browsers": {"Chrome": 9}, "os": {"macOS": 9}, "devices": {"desktop": 9}}},
        ),
    ],
)
def test_rum_models_roundtrip(model_name, payload):
    validated = getattr(rum_models, model_name).model_validate(payload)
    assert validated.model_dump(mode="json") == payload and validated.model_dump_json() == json.dumps(
        payload, separators=(",", ":")
    )


# ── Remote share admin & frontend deployment ────────────────────────────────


@pytest.fixture
def remote_share_client():
    mini = FastAPI()
    mini.add_middleware(RemoteAccessMiddleware)
    mini.include_router(share_admin.router)
    with TestClient(mini) as c:
        yield c


@pytest.fixture
def analyst_dep_client():
    class _StampAnalystMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            request.state.analyst_session = _make_analyst_session()
            return await call_next(request)

    mini = FastAPI()
    mini.add_middleware(_StampAnalystMiddleware)
    mini.include_router(share_admin.router)
    with TestClient(mini) as c:
        yield c


def test_share_status_live_start_stop_settings_and_lifecycle(remote_share_client: TestClient):
    status_keys = {"sharing_active", "public_endpoint", "public_url", "active_session_count", "max_concurrent_sessions"}
    status_keys |= {"invites", "sessions", "audit_logs", "services", "rate_limits", "telemetry"}
    assert status_keys <= set(_body(remote_share_client.get("/api/admin/share/status")))
    assert {"sharing_active", "public_url", "active_session_count", "rate_limits", "telemetry"} <= set(
        _body(remote_share_client.get("/api/admin/share/live"))
    )

    r_bad = remote_share_client.post("/api/admin/share/start", json={"public_endpoint": "http://insecure.example.com"})
    assert r_bad.status_code == 400 and r_bad.json()["detail"]["error"] == "invalid_request"
    r_start = remote_share_client.post("/api/admin/share/start", json={"public_endpoint": "https://share.example.com"})
    assert _body(r_start)["public_url"] == "https://share.example.com"

    live_on = _body(remote_share_client.get("/api/admin/share/live"))
    assert live_on["sharing_active"] is True and live_on["public_url"] == "https://share.example.com"
    assert _body(remote_share_client.post("/api/admin/share/stop"))["ok"] is True

    r_patch = remote_share_client.patch("/api/admin/share/settings", json={"max_concurrent_analyst_sessions": 15})
    assert _body(r_patch)["max_concurrent_analyst_sessions"] == 15
    assert _body(remote_share_client.get("/api/admin/share/settings"))["max_concurrent_analyst_sessions"] == 15
    assert {"version", "text"} <= set(_body(remote_share_client.get("/api/admin/share/tos")))

    for method, ver, text in (
        ("post", "v2.0-test", "# Custom Terms of Use\n\nConfidential analyst terms v2 for testing."),
        ("put", "v3.0-test", "# Custom Terms of Use\n\nConfidential analyst terms v3 for testing."),
    ):
        res = _body(getattr(remote_share_client, method)("/api/admin/share/tos", json={"version": ver, "text": text}))
        assert res["version"] == ver and res["text"] == text
    assert remote_share_client.post("/api/admin/share/tos", json={"version": "   ", "text": ""}).status_code == 400

    inv_req = {"name": "Alice Analyst", "email": "alice@example.com", "passcode": "ocean-breeze-cabin-42"}
    inv_req |= {"duration_hours": 48, "service_ids": ["svc-alpha"], "pii_policy": {"mask_ips": True}}
    invite = _body(remote_share_client.post("/api/admin/share/invites", json=inv_req))
    invite_id = invite["id"]
    assert invite["email"] == "alice@example.com" and invite["revoked"] == 0 and invite["service_ids"] == ["svc-alpha"]
    assert any(row["id"] == invite_id for row in _body(remote_share_client.get("/api/admin/share/invites"))["invites"])

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

    mgr = get_tunnel_manager()
    for sid, action in (("sess-revoke-1", "revoke"), ("sess-boot-2", "boot")):
        mgr._sessions[sid] = _make_analyst_session(session_id=sid, invite_id="inv-1", service_ids=["svc-alpha"])
        assert _body(remote_share_client.post(f"/api/admin/share/sessions/{sid}/{action}"))["ok"] is True
        assert sid not in mgr._sessions
        assert remote_share_client.post(f"/api/admin/share/sessions/missing-sess/{action}").status_code == 404

    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS", email="filter-target@example.com", ip_address="192.0.2.1", details="OK"
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="other-user@example.com", ip_address="192.0.2.2", details="Bad"
    )
    audit_params = {"event_type": "LOGIN_SUCCESS", "email": "filter-target@example.com", "limit": 50}
    rows = _body(remote_share_client.get("/api/admin/share/audit-logs", params=audit_params))["audit_logs"]
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
    analyst_dep_client: TestClient, method: str, path: str, json_body: dict | None
):
    resp = analyst_dep_client.request(method, path, json=json_body)
    assert resp.status_code == 403, f"Expected 403 on {method} {path}, got {resp.status_code}: {resp.text}"
    assert resp.json().get("detail", resp.json()).get("error") == "admin_only"


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
    dom = "test-domain.global.ssl.fastly.net"
    payload = {"service_name": "test-svc", "domain_name": dom, "origin_host": "203.0.113.10", **extra_payload}
    mock_res = {"service_id": "svc_1", "version": 1, "domain_name": dom, "origin_host": "203.0.113.10"}
    with (
        patch("backend.routers.share_admin.list_configs", return_value=configs),
        patch.dict(os.environ, {"FASTLY_API_KEY": env_token} if env_token else {}, clear=env_token is None),
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
    ):
        assert _body(client.post("/api/sharing/deploy-frontend", json=payload)) == mock_res
    mock_deploy.assert_called_once_with(
        service_name="test-svc",
        domain_name=dom,
        origin_host="203.0.113.10",
        origin_port=80,
        use_ssl=False,
        token=expected_token,
        override_host=expected_override_host,
    )


def test_deploy_and_teardown_frontend_persistence_and_errors(client):
    dom = "test-domain.global.ssl.fastly.net"
    base_payload = {"service_name": "test-service", "domain_name": dom, "origin_host": "203.0.113.10"}
    mock_res = {"service_id": "service_remote_id", "version": 1, "domain_name": dom, "origin_host": "203.0.113.10"}
    with patch("backend.routers.share_admin.list_configs", return_value=[]), patch.dict(os.environ, {}, clear=True):
        r_no_tok = client.post("/api/sharing/deploy-frontend", json=base_payload)
        assert r_no_tok.status_code == 400 and "Token is required" in r_no_tok.json()["detail"]["message"]
    r_422 = client.post(
        "/api/sharing/deploy-frontend", json={"service_name": "s", "domain_name": dom, "token_override": "t"}
    )
    assert r_422.status_code == 422
    with patch("backend.routers.share_admin.deploy_remote_frontend", side_effect=RuntimeError("Fastly API error")):
        r_500 = client.post("/api/sharing/deploy-frontend", json={**base_payload, "token_override": "some-token"})
        assert r_500.status_code == 500 and r_500.json()["detail"]["error"] == "request_failed"

    saved: dict = {}
    svc_payload = {**base_payload, "token_override": "token-123", "service_id": "logging-service-123"}
    with patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res):
        with patch(
            "backend.config.mutate_config", side_effect=fake_mutate_config({"service_id": "logging-service-123"}, saved)
        ) as mock_mutate:
            assert _body(client.post("/api/sharing/deploy-frontend", json=svc_payload)) == mock_res
            mock_mutate.assert_called_once()
            assert saved["remote_frontend"] == mock_res
        with patch("backend.config.mutate_config", side_effect=RuntimeError("filesystem full")):
            assert _body(client.post("/api/sharing/deploy-frontend", json=svc_payload)) == mock_res

    rf = {"service_id": "remote_service_123", "version": 1, "domain_name": dom, "origin_host": "203.0.113.10"}
    mock_config = {"service_id": "service123", "remote_frontend": rf}
    saved_td: dict = {}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=dict(mock_config)),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config", side_effect=fake_mutate_config(dict(mock_config), saved_td)),
    ):
        td_req = {"service_id": "service123", "token_override": "teardown-token"}
        assert _body(client.post("/api/sharing/teardown-frontend", json=td_req))["ok"] is True
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="teardown-token")
        assert saved_td == {"service_id": "service123"}

    with (
        patch("backend.utils.router_utils.load_service_config", return_value={"service_id": "service123"}),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config") as mock_mutate,
    ):
        assert (
            _body(
                client.post("/api/sharing/teardown-frontend", json={"service_id": "service123", "token_override": "t"})
            )["ok"]
            is True
        )
        mock_delete.assert_not_called()
        mock_mutate.assert_not_called()

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
            assert client.post("/api/sharing/teardown-frontend", json={"service_id": "service123"}).status_code == 200
            mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token=expected_tok)

    with patch("backend.routers.share_admin.list_configs", return_value=[]), patch.dict(os.environ, {}, clear=True):
        r_400 = client.post("/api/sharing/teardown-frontend", json={"service_id": "service123"})
        assert r_400.status_code == 400 and "Token is required" in r_400.json()["detail"]["message"]

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


def test_schema_endpoint_durable_mode_uses_catalog_without_duckdb_con(client, test_service_source):
    from backend.core.duckdb_status import _clear_schema_cache

    _clear_schema_cache()
    with (
        patch("backend.config.get_status", return_value={}),
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.core.duckdb.get_schema", side_effect=AssertionError("should not call get_schema")),
    ):
        resp = client.get("/api/schema", headers=_SVC_HDR)
        assert resp.status_code == 200
        schema_cols = {c["name"] for c in resp.json()["schema"]}
        assert "timestamp" in schema_cols and "url" in schema_cols
        # Second call hits _schema_cache directly
        resp2 = client.get("/api/schema", headers=_SVC_HDR)
        assert resp2.status_code == 200
    _clear_schema_cache()


def test_correlate_filters_to_4xx_5xx_error_cohort_in_standard_and_high_scale(in_memory_duckdb):
    in_memory_duckdb.execute("CREATE TABLE logs_cr_err_test (timestamp TIMESTAMPTZ, url VARCHAR, status INTEGER)")
    ts = datetime(2026, 10, 20, 11, 30, tzinfo=UTC)
    in_memory_duckdb.executemany(
        "INSERT INTO logs_cr_err_test VALUES (?, ?, ?)",
        [
            (ts, "/ok", 200),
            (ts, "/ok", 200),
            (ts, "/redir", 302),
            (ts, "/notfound", 404),
            (ts, "/notfound", 404),
            (ts, "/notfound", 404),
            (ts, "/boom", 503),
        ],
    )
    try:
        rows, latest_ts = cr_mod.correlate_top_values(
            in_memory_duckdb,
            "logs_cr_err_test",
            "url",
            datetime(2026, 10, 20, 11, 0, tzinfo=UTC),
            10,
            service_id="svc_cr",
        )
        assert rows == [("/notfound", 3), ("/boom", 1)]
        assert latest_ts is not None
    finally:
        in_memory_duckdb.execute("DROP TABLE IF EXISTS logs_cr_err_test")

    captured_ch: list[tuple[str, dict]] = []

    class _HSClient:
        def execute(self, sql: str, params: dict | None = None):
            captured_ch.append((sql, params or {}))
            return [{"value": "/notfound", "aggregate_count": 3}, {"value": "/boom", "aggregate_count": 1}]

    hs_svc = SimpleNamespace(
        service_id=MOCK_SERVICE_ID,
        client=_HSClient(),
        watermark_for=lambda _dom: SimpleNamespace(coverage_end=ts, validate=lambda: None),
    )
    fake_reg = SimpleNamespace(resolve=lambda sid: hs_svc if sid == MOCK_SERVICE_ID else None)
    resp = asyncio.run(
        cr_mod.control_room_correlate(
            service_id=MOCK_SERVICE_ID,
            body=cr_mod.CorrelateRequest(dimension="url", window_minutes=15, limit=10),
            request=SimpleNamespace(state=SimpleNamespace(analyst_session=None)),  # type: ignore[arg-type]
            registry=fake_reg,  # type: ignore[arg-type]
        )
    )
    assert resp["top"] == [
        {"value": "/notfound", "count": 3},
        {"value": "/boom", "count": 1},
    ]
    top_sql, top_params = captured_ch[0]
    assert "FROM request_facts" in top_sql
    assert "toInt32OrZero(custom_fields['status']) BETWEEN 400 AND 599" in top_sql
    assert "high_scale_batch_publications FINAL" in top_sql and "publication_state='visible'" in top_sql
    assert top_params["service_id"] == MOCK_SERVICE_ID and top_params["limit"] == 10
