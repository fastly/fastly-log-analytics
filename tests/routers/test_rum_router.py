"""Consolidated unit tests for RUM admin, analytics, vitals, assets, and telemetry routers."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import typing
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest
import time_machine
from backend.utils.time_window import resolve_window
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.core import request_telemetry as web_vitals_store
from backend.core.request_context import RequestContext
from backend.main import app
from backend.provision.rum_orchestrator_v2 import upgrade_faro_version
from backend.routers import rum as rum_router
from backend.routers.rum import router as _rum_api_router
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.remote_access import TimeBounds, _is_blocked_path, get_analyst_time_bounds
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.rum import insert_rum_beacons as _insert_beacons
from tests._helpers.rum import setup_temp_rum_db
from tests.conftest import make_test_request_context

__all__ = ["setup_temp_rum_db"]

SVC = "svc-rum-admin-test"
GOLDEN_DIR = Path(__file__).parent / "golden"
_GOLDEN_SERVICE_ID = "svc-rum-golden"
_TOKEN_SERVICE_ID = "svc-rum-token"
WINDOW = "start_time=2026-08-05T00:00:00Z&end_time=2026-08-05T23:59:59Z"
ANCHOR = "2026-08-05T12:00:30Z"
EMPTY = {"no_data": True, "vitals_rows": []}

client = TestClient(app)


def _parse_sse(text: str) -> list[dict[str, Any]]:
    return [
        json.loads(line[len("data:") :].strip())
        for raw_line in text.splitlines()
        if (line := raw_line.strip()).startswith("data:") and line[len("data:") :].strip()
    ]


def _make_app(*, analyst: bool = False) -> FastAPI:
    test_app = FastAPI()
    if analyst:

        @test_app.middleware("http")
        async def _inject_analyst(request: Request, call_next):
            request.state.analyst_session = MagicMock(session_id="sess-analyst-rum")
            return await call_next(request)

    test_app.include_router(_rum_api_router)
    return test_app


def _fake_request(analyst_session: object | None) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _mock_rum_source(db_path: Any) -> dict[str, str]:
    return {
        "name": "test_service",
        "service_id": "test_service",
        "duckdb_path": str(db_path).replace(".rum.duckdb", ".duckdb"),
        "access_level": "read_write",
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }


@pytest.fixture
def skip_view_update(request):
    if request.node.name in ("test_rum_analytics_cold_start_readonly", "test_rum_live_buffer_unlink_race_self_heals"):
        yield None
    else:
        with patch("backend.core.iceberg.view.update_iceberg_view") as mock:
            yield mock


@pytest.fixture
def _reset_faro_cache():
    from backend.core import faro_versions

    faro_versions.reset_versions_cache()
    yield
    faro_versions.reset_versions_cache()


# ── Section 1: RUM Status & Analyst Projection ───────────────────────────────


@pytest.mark.parametrize(
    ("cfg", "cur_sha", "leg_sha", "expected_drift", "expected_deployed", "expect_save"),
    [
        (
            {
                "rum_enabled": True,
                "rum_enabled_at": "2026-10-18T08:00:00Z",
                "rum_vcl_sha": "sha-current",
                "rum": {
                    "enabled": True,
                    "enabled_at": "2026-10-18T08:00:00Z",
                    "deployed_vcl_sha": "sha-current",
                    "capture_vitals": True,
                    "capture_performance": False,
                    "capture_errors": True,
                    "capture_events": False,
                    "custom_condition": 'req.url.ext != "jpg"',
                },
            },
            "sha-current",
            "sha-legacy",
            False,
            "sha-current",
            False,
        ),
        (
            {
                "rum_enabled": True,
                "rum_enabled_at": "2026-10-18T08:00:00Z",
                "rum_vcl_sha": "sha-old-deployed",
                "rum": {"enabled": True},
            },
            "sha-new-template",
            "sha-legacy-unrelated",
            True,
            "sha-old-deployed",
            False,
        ),
        (
            {
                "rum_enabled": True,
                "rum_enabled_at": "2026-10-18T08:00:00Z",
                "rum_vcl_sha": "sha-legacy-match",
                "rum": {"enabled": True},
            },
            "sha-new-v2",
            "sha-legacy-match",
            False,
            "sha-new-v2",
            True,
        ),
        (
            {"rum_enabled": True, "rum_enabled_at": "2026-01-01T00:00:00Z"},
            "freshsha123",
            "sha-legacy",
            False,
            "freshsha123",
            False,
        ),
    ],
)
def test_admin_rum_status_variants(cfg, cur_sha, leg_sha, expected_drift, expected_deployed, expect_save):
    test_client = TestClient(_make_app(analyst=False))
    mock_save = MagicMock()
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save)),
        patch("backend.routers.rum.rum_vcl_fingerprint", return_value=cur_sha),
        patch("backend.routers.rum.legacy_rum_vcl_fingerprint", return_value=leg_sha),
    ):
        res = test_client.get(f"/api/services/{SVC}/rum/status")

    assert res.status_code == 200
    body = res.json()
    assert body["enabled"] is True and body["vcl_drift"] is expected_drift
    assert body["deployed_vcl_sha"] == expected_deployed and body["current_vcl_sha"] == cur_sha
    assert mock_save.called is expect_save
    if "custom_condition" in cfg.get("rum", {}):
        assert body["custom_condition"] == 'req.url.ext != "jpg"' and body["capture_performance"] is False


@pytest.mark.security_regression
async def test_analyst_status_excludes_vcl_fingerprint_fields(with_config, monkeypatch):
    svc = "svc-rum-status-projection"
    with_config[svc] = {"rum_enabled": True, "rum_enabled_at": "2026-01-01T00:00:00Z", "rum_vcl_sha": "deadbeef"}
    monkeypatch.setattr(
        rum_router,
        "rum_vcl_fingerprint",
        lambda service_id: pytest.fail("rum_vcl_fingerprint must not be called for an analyst caller"),
    )
    sess = SimpleNamespace(session_id="s-test", service_ids=[svc], email="analyst@example.com")
    result = await rum_router.rum_status(_fake_request(sess), service_id=svc)
    assert result == {"enabled": True, "enabled_at": "2026-01-01T00:00:00Z"}
    for forbidden in ("deployed_vcl_sha", "current_vcl_sha", "vcl_drift"):
        assert forbidden not in result


@pytest.mark.security_regression
async def test_analyst_can_read_enabled_field(with_config):
    svc = "svc-rum-status-projection"
    with_config[svc] = {"rum_enabled": False}
    sess = SimpleNamespace(session_id="s-test", service_ids=[svc], email="analyst@example.com")
    result = await rum_router.rum_status(_fake_request(sess), service_id=svc)
    assert result["enabled"] is False and "enabled_at" in result


# ── Section 2: RUM Settings, Versions, Enable/Disable/Upgrade & RBAC ─────────


@pytest.mark.parametrize(("custom_condition", "expect_reconcile"), [("", False), ('req.url !~ "^/healthz"', True)])
def test_update_settings_reconcile_and_missing(custom_condition: str, expect_reconcile: bool):
    test_client = TestClient(_make_app(analyst=False))
    with patch("backend.config.load_config", return_value=None):
        assert test_client.post(f"/api/services/{SVC}/rum/settings", json={"capture_vitals": True}).status_code == 404

    cfg = {
        "fastly_api_key": "tok-stored",
        "rum_enabled": True,
        "rum": {
            "enabled": True,
            "capture_vitals": True,
            "capture_performance": True,
            "capture_errors": True,
            "capture_events": True,
            "custom_condition": "",
        },
    }
    payload = {
        "capture_vitals": True,
        "capture_performance": False,
        "capture_errors": True,
        "capture_events": False,
        "custom_condition": custom_condition,
    }
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
        patch("backend.provision.rum_assets.upload_rum_tracker_js", return_value={"skipped": False}) as mock_upload,
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile,
    ):
        res = test_client.post(f"/api/services/{SVC}/rum/settings", json=payload)
    assert res.status_code == 200 and res.json()["ok"] is True
    mock_save.assert_called_once()
    mock_upload.assert_called_once_with(SVC, "tok-stored", overwrite=True)
    assert mock_reconcile.called is expect_reconcile


@pytest.mark.parametrize(
    ("upload_err", "reconcile_err", "custom_condition", "expected_error_code"),
    [
        (RuntimeError("FOS bucket unreachable"), None, "", "rum_tracker_upload_failed"),
        (None, RuntimeError("Fastly VCL compile error"), 'req.http.X-Test == "1"', "rum_reconcile_failed"),
    ],
)
def test_update_settings_rolls_back_on_failure(upload_err, reconcile_err, custom_condition, expected_error_code):
    cfg = {
        "fastly_api_key": "tok-stored",
        "rum_enabled": True,
        "rum": {"enabled": True, "capture_vitals": True, "custom_condition": ""},
    }
    test_client = TestClient(_make_app(analyst=False))
    payload = {
        "capture_vitals": False,
        "capture_performance": False,
        "capture_errors": False,
        "capture_events": False,
        "custom_condition": custom_condition,
    }
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
        patch(
            "backend.provision.rum_assets.upload_rum_tracker_js",
            side_effect=upload_err if upload_err else (lambda *a, **kw: {"skipped": False}),
        ),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state", side_effect=reconcile_err),
    ):
        res = test_client.post(f"/api/services/{SVC}/rum/settings", json=payload)
    assert res.status_code == 500 and res.json()["detail"]["error"] == expected_error_code
    assert mock_save.call_count == 2
    rollback_cfg = mock_save.call_args_list[1].args[1]
    assert rollback_cfg["rum"]["capture_vitals"] is True and rollback_cfg["rum"]["custom_condition"] == ""


@pytest.mark.usefixtures("_reset_faro_cache")
@pytest.mark.parametrize(
    ("svc_cfg", "fetched", "expected"),
    [
        (
            {"rum": {"faro_version": "2.8.2"}},
            ["2.9.0", "2.8.2", "2.8.1"],
            {"available": ["2.9.0", "2.8.2", "2.8.1"], "current": "2.8.2", "latest": "2.9.0", "update_available": True},
        ),
        (
            {"rum": {"faro_version": "2.9.0"}},
            ["2.9.0", "2.8.2"],
            {"available": ["2.9.0", "2.8.2"], "current": "2.9.0", "latest": "2.9.0", "update_available": False},
        ),
        (
            {},
            ["2.9.0", "2.8.2"],
            {"available": ["2.9.0", "2.8.2"], "current": None, "latest": "2.9.0", "update_available": False},
        ),
        (
            {"rum": {"faro_version": "2.8.2"}},
            ValueError("registry 503"),
            {"available": [], "current": "2.8.2", "latest": None, "update_available": False},
        ),
    ],
)
def test_rum_versions_variants(with_config, monkeypatch, svc_cfg, fetched, expected):
    svc = "TestRumVersionsUpgradeSvc"
    with_config[svc] = {"service_id": svc, **svc_cfg}

    async def fake_fetch():
        if isinstance(fetched, Exception):
            raise fetched
        return fetched

    monkeypatch.setattr("backend.core.faro_versions.fetch_available_faro_versions", fake_fetch)
    r = client.get(f"/api/services/{svc}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    for k, v in expected.items():
        assert body.get(k) == v


@pytest.mark.parametrize(
    ("action", "activate", "final_enabled", "expected_msg_substr"),
    [
        ("enable", True, True, "enabled successfully"),
        ("enable", False, False, "draft configuration compiled"),
        ("disable", True, False, "disabled successfully"),
        ("disable", False, False, "draft configuration compiled"),
    ],
)
def test_enable_disable_sse_modes(
    with_config, monkeypatch, action: str, activate: bool, final_enabled: bool, expected_msg_substr: str
):
    svc = "TestRumEnableDisableSvc"
    with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN", "rum_enabled": not final_enabled}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": f"{action}ing"}
        with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN", "rum_enabled": final_enabled}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: SimpleNamespace(reload=lambda: None))

    r = client.post(f"/api/services/{svc}/rum/{action}", json={"activate": activate})
    assert r.status_code == 200
    done = next(e for e in _parse_sse(r.text) if e["type"] == "done")
    assert done["rum"]["enabled"] is final_enabled and expected_msg_substr in done["message"]


@pytest.mark.parametrize("action", ["enable", "disable"])
def test_enable_disable_errors_and_resilience(with_config, monkeypatch, action: str):
    svc = "TestRumEnableDisableSvc"
    assert client.post(f"/api/services/{svc}/rum/{action}", json={}).status_code == 404
    with_config[svc] = {"service_id": svc, "fastly_api_key": ""}
    assert client.post(f"/api/services/{svc}/rum/{action}", json={}).status_code == 400

    monkeypatch.setattr(
        "backend.routers.rum.run_with_events", lambda f, *a, **kw: iter([{"type": "status", "message": "ok"}])
    )
    monkeypatch.setattr(
        "backend.cron.scheduler.get_scheduler", lambda: (_ for _ in ()).throw(RuntimeError("scheduler down"))
    )
    r = client.post(f"/api/services/{svc}/rum/{action}", json={"token": "SUPPLIED-TOKEN"})
    assert r.status_code == 200 and any(e["type"] == "done" for e in _parse_sse(r.text))
    if action == "enable":
        assert with_config[svc]["fastly_api_key"] == "SUPPLIED-TOKEN"

    with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN"}

    def _boom_events(func, *args, **kwargs):
        yield {"type": "status", "message": "starting"}
        raise RuntimeError(f"{action} orchestrator boom")

    monkeypatch.setattr("backend.routers.rum.run_with_events", _boom_events)
    events = _parse_sse(client.post(f"/api/services/{svc}/rum/{action}", json={}).text)
    assert any(e["type"] == "error" and f"{action} orchestrator boom" in e["message"] for e in events)
    assert not any(e["type"] == "done" for e in events)


@pytest.mark.usefixtures("_reset_faro_cache")
def test_upgrade_validation_and_sse_lifecycle(with_config, monkeypatch):
    svc = "TestRumVersionsUpgradeSvc"
    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", AsyncMock(return_value=["2.9.0", "2.8.2"]))

    with_config[svc] = {"service_id": svc, "fastly_api_key": ""}
    r_notok = client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "2.9.0"})
    assert r_notok.status_code == 400 and "api key" in r_notok.json()["detail"]["error"].lower()

    with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN"}
    mock_run = MagicMock()
    monkeypatch.setattr("backend.routers.rum.run_with_events", mock_run)
    r_unk = client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "9.9.9"})
    assert r_unk.status_code == 400 and r_unk.json()["detail"]["error"] == "unknown_faro_version"
    mock_run.assert_not_called()

    captured: dict[str, Any] = {}

    def _ok_events(func, *args, **kwargs):
        captured.update(func=func, args=args, kwargs=kwargs)
        yield {"type": "status", "message": "downloading v2.9.0"}
        with_config[svc] = {"service_id": svc, "fastly_api_key": "TOKEN", "rum": {"faro_version": "2.9.0"}}

    monkeypatch.setattr("backend.routers.rum.run_with_events", _ok_events)
    r_ok = client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "2.9.0"})
    assert r_ok.status_code == 200
    done = next(e for e in _parse_sse(r_ok.text) if e["type"] == "done")
    assert done["rum"]["faro_version"] == "2.9.0"
    assert captured["func"] is upgrade_faro_version and captured["args"] == (svc, "2.9.0", "TOKEN")
    assert captured["kwargs"]["activate"] is True

    def _err_events(func, *args, **kwargs):
        yield {"type": "status", "message": "downloading"}
        raise RuntimeError("upload failed: FOS 503")

    monkeypatch.setattr("backend.routers.rum.run_with_events", _err_events)
    events = _parse_sse(client.post(f"/api/services/{svc}/rum/upgrade", json={"version": "2.9.0"}).text)
    assert any(e["type"] == "error" and "fos" in e["message"].lower() for e in events)
    assert not any(e["type"] == "done" for e in events)


class TestRumAdminRbacEnforcement:
    """Analyst Path B sessions must receive 403 {"error": "admin_only"} on all RUM admin/mutation endpoints."""

    @pytest.mark.parametrize(
        ("method", "suffix", "payload"),
        [
            ("POST", "enable", {"activate": True}),
            ("POST", "disable", {"activate": True}),
            ("GET", "versions", None),
            ("POST", "upgrade", {"version": "2.3.1", "activate": True}),
            ("POST", "settings", {"capture_vitals": True}),
        ],
    )
    def test_analyst_blocked_at_middleware_and_route_dependency(
        self, method: str, suffix: str, payload: dict[str, Any] | None
    ):
        path = f"/api/services/{SVC}/rum/{suffix}"
        assert _is_blocked_path(path) is True
        test_client = TestClient(_make_app(analyst=True))
        res = test_client.get(path) if method == "GET" else test_client.post(path, json=payload)
        assert res.status_code == 403 and res.json()["detail"]["error"] == "admin_only"


# ── Section 3: RUM Analytics, Vitals, Filters, Beacon Health & Goldens ───────


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_fallback_and_live_events(setup_temp_rum_db) -> None:
    response = client.get("/api/services/svc-test-rum-fallback/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["no_data"] is True and data["beacon_count"] == 0
    assert data["worst_pages"] == [] and data["errors"] == [] and data["trends"]["lcp"] == []
    assert client.get("/api/services/svc-test-rum-fallback/rum/live-events").json() == []

    _insert_beacons(
        setup_temp_rum_db,
        [
            {
                "pathname": "/live-test",
                "load_time": 1.5,
                "lcp": 1.7,
                "cls": 0.01,
                "inp": 100,
                "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            }
        ],
    )
    events = client.get("/api/services/svc-test-rum-fallback/rum/live-events").json()
    assert len(events) == 4 and events[0]["path"] == "/live-test"
    assert events[0]["browser"] == "Chrome" and events[0]["os"] == "macOS"


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_real_data(setup_temp_rum_db) -> None:
    service_id = "test_service_real_data"
    err_null = [{"value": "TypeError: null is not an object", "filename": "main.js"}]
    err_ref = [{"value": "ReferenceError: x is not defined", "filename": "checkout.js"}]
    rows = [
        ("/pricing", 2.1, 2.3, 0.02, 120, "Chrome", "macOS", "Desktop", err_null, ""),
        ("/", 1.4, 1.6, 0.01, 80, "Chrome", "macOS", "Desktop", None, None),
        ("/pricing", 2.5, 2.8, 0.03, 150, "Safari", "iOS", "Mobile", None, None),
        ("/docs", 1.8, 1.9, 0.05, 90, "Firefox", "Linux", "Desktop", None, None),
        ("/", 1.2, 1.4, 0.01, 70, "Chrome", "Windows", "Desktop", None, None),
        ("/checkout", 3.2, 3.5, 0.12, 220, "Chrome", "macOS", "Desktop", err_null, None),
        ("/pricing", 2.0, 2.2, 0.02, 110, "Chrome", "macOS", "Desktop", None, None),
        ("/", 1.5, 1.7, 0.02, 85, "Safari", "macOS", "Desktop", None, None),
        ("/docs", 1.9, 2.1, 0.04, 100, "Chrome", "Android", "Mobile", None, None),
        ("/", 1.3, 1.5, 0.01, 75, "Edge", "Windows", "Desktop", None, None),
        ("/pricing", 2.2, 2.4, 0.02, 130, "Chrome", "macOS", "Desktop", None, None),
        ("/checkout", 3.0, 3.2, 0.10, 200, "Chrome", "macOS", "Desktop", err_ref, None),
    ]
    test_beacons = [
        {
            "pathname": p,
            "load_time": lt,
            "lcp": lcp,
            "cls": cls,
            "inp": inp,
            "meta": {"browser": b, "os": o, "device": d},
            **({"exceptions": exc} if exc is not None else {}),
            **({"req_id": rid} if rid is not None else {}),
        }
        for p, lt, lcp, cls, inp, b, o, d, exc, rid in rows
    ]
    _insert_beacons(setup_temp_rum_db, test_beacons)

    response = client.get(f"/api/services/{service_id}/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False
    assert data["vitals"]["lcp"]["distribution"]["good"] > 0 and data["vitals"]["lcp"]["distribution"]["poor"] == 0
    assert data["environments"]["browsers"].get("Chrome", 0) == 8
    assert data["environments"]["os"].get("macOS", 0) == 7
    assert data["environments"]["devices"].get("Desktop", 0) == 10
    assert [e["count"] for e in data["errors"]] == [2, 1]
    assert len(client.get(f"/api/services/{service_id}/rum/live-events").json()) >= 12


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize("window_hours", [0.5, 24])
def test_rum_analytics_serves_without_in_request_recompute(setup_temp_rum_db, window_hours: float) -> None:
    import backend.core.duckdb as _duckdb_mod

    service_id = f"test_service_no_recompute_{window_hours}"
    now = datetime.datetime.now(datetime.UTC)
    offset_mins = 2 if window_hours < 1 else 60
    _insert_beacons(
        setup_temp_rum_db,
        [
            {
                "pathname": f"/p_{i}",
                "load_time": 1.4,
                "lcp": 1.8,
                "cls": 0.02,
                "received_at": (now - datetime.timedelta(minutes=offset_mins + i)).isoformat(),
                "req_id": f"rid_{i}",
            }
            for i in range(12)
        ],
    )
    start, end = iso_z(now - datetime.timedelta(hours=window_hours)), iso_z(now)
    real_get_connection = _duckdb_mod.get_connection
    write_conn_calls = []

    def _spy_get_connection(*args, **kwargs):
        if kwargs.get("read_only") is False:
            write_conn_calls.append((args, kwargs))
        return real_get_connection(*args, **kwargs)

    with (
        patch("backend.core.rollups.rum.recompute_rum_aggregates") as mock_recompute,
        patch.object(_duckdb_mod, "get_connection", side_effect=_spy_get_connection),
    ):
        response = client.get(f"/api/services/{service_id}/rum/analytics?start_time={start}&end_time={end}")

    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["beacon_count"] > 0
    mock_recompute.assert_not_called()
    assert write_conn_calls == []


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_date_filtering_and_shared_distinct_id(setup_temp_rum_db) -> None:
    service_id = "test_service_date_filter"
    _insert_beacons(
        setup_temp_rum_db,
        [
            {
                "pathname": f"/page_{i}",
                "load_time": 1.5,
                "received_at": "2026-08-05T10:00:00+00:00" if i < 10 else "2026-08-05T12:00:00+00:00",
            }
            for i in range(20)
        ],
    )
    data_after = client.get(f"/api/services/{service_id}/rum/analytics?start_time=2026-08-05T11:00:00+00:00").json()
    assert all(int(p["path"].split("_")[1]) >= 10 for p in data_after["worst_pages"])
    data_before = client.get(f"/api/services/{service_id}/rum/analytics?end_time=2026-08-05T11:00:00+00:00").json()
    assert all(int(p["path"].split("_")[1]) < 10 for p in data_before["worst_pages"])

    con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
    try:
        con.execute("DELETE FROM client_vitals; DELETE FROM client_errors;")
        shared_ts = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
        other_ts = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=6)
        con.executemany(
            "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    shared_ts,
                    "lcp",
                    2.1,
                    "good",
                    "/",
                    "Chrome",
                    "macOS",
                    "Desktop",
                    "shared",
                    "",
                    "Austin",
                    "TX",
                    "US",
                    "SIN",
                    "1.3",
                    45.0,
                ),
                (
                    shared_ts,
                    "event_click",
                    1.0,
                    "",
                    "/",
                    "Chrome",
                    "macOS",
                    "Desktop",
                    "shared",
                    "",
                    "Austin",
                    "TX",
                    "US",
                    "SIN",
                    "1.3",
                    45.0,
                ),
            ],
        )
        con.execute(
            "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                other_ts,
                "TypeError",
                "main.js",
                0,
                0,
                "/",
                "Chrome",
                "macOS",
                "Desktop",
                "other",
                "",
                "Austin",
                "TX",
                "US",
                "SIN",
                "1.3",
                45.0,
            ),
        )
    finally:
        con.close()

    dedup = client.get("/api/services/test_service_shared_distinct_id/rum/analytics").json()
    assert dedup["beacon_count"] == 2 and dedup["pageview_count"] == 1 and dedup["error_count"] == 1


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize(
    ("mode", "browser_val", "expected_browsers", "expected_count"),
    [
        ("include", "Chrome", {"Chrome": 8}, 8),
        ("include", "Safari", {"Safari": 7}, 7),
        ("include", "Firefox", {}, 0),
        ("exclude", "Chrome", {"Safari": 7}, 7),
    ],
)
def test_rum_analytics_with_filters(
    setup_temp_rum_db, mode: str, browser_val: str, expected_browsers: dict, expected_count: int
) -> None:
    service_id = "test_service_with_filters"
    _insert_beacons(
        setup_temp_rum_db,
        [
            {
                "pathname": f"/page_{i}",
                "load_time": 2.0,
                "lcp": 2.1,
                "cls": 0.02,
                "inp": 100,
                "meta": {
                    "browser": "Chrome" if i < 8 else "Safari",
                    "os": "macOS" if i < 10 else "iOS",
                    "device": "Desktop" if i < 12 else "Mobile",
                },
            }
            for i in range(15)
        ],
    )
    filters = json.dumps({"browser": {"mode": mode, "values": [browser_val]}})
    response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False and data["beacon_count"] == expected_count
    if expected_browsers:
        assert data["environments"]["browsers"] == expected_browsers


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_analyst_request_outside_invite_window_is_clamped(setup_temp_rum_db) -> None:
    service_id = "test_rum_analytics_clamp_analyst"
    old_ts = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=30)).isoformat()
    recent_ts = datetime.datetime.now(datetime.UTC).isoformat()
    _insert_beacons(
        setup_temp_rum_db,
        [
            {"pathname": "/thirty-days-old", "load_time": 1.5, "received_at": old_ts},
            {"pathname": "/just-now", "load_time": 1.5, "received_at": recent_ts},
        ],
    )
    src = _mock_rum_source(setup_temp_rum_db)
    analyst_sess = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    analyst_req = _fake_request(analyst_sess)
    analyst_ctx = make_test_request_context(
        service_id=service_id,
        source=src,
        session=analyst_sess,
        read_only=True,
        time_bounds=get_analyst_time_bounds(typing.cast(typing.Any, analyst_req)),
    )
    result = asyncio.run(
        rum_router.rum_analytics(
            request=typing.cast(typing.Any, analyst_req),
            start_time="2000-01-01T00:00:00Z",
            end_time=None,
            filters=None,
            ctx=analyst_ctx,
        )
    )
    assert "/thirty-days-old" not in [p["path"] for p in result["worst_pages"]]

    admin_ctx = make_test_request_context(
        service_id=service_id, source=src, session=None, read_only=True, time_bounds=TimeBounds(None, None)
    )
    admin_result = asyncio.run(
        rum_router.rum_analytics(
            request=typing.cast(typing.Any, _fake_request(None)),
            start_time="2000-01-01T00:00:00Z",
            end_time=None,
            filters=None,
            ctx=admin_ctx,
        )
    )
    assert "/thirty-days-old" in [p["path"] for p in admin_result["worst_pages"]]


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_cold_start_readonly(tmp_path):
    from backend.core.iceberg.view import update_iceberg_view

    temp_db_path = tmp_path / "cold_start.rum.duckdb"
    duckdb.connect(str(temp_db_path)).close()
    ro_con = duckdb.connect(str(temp_db_path), read_only=True)
    try:
        source = {
            "name": "cold_start::rum",
            "service_id": "cold_start",
            "duckdb_path": str(tmp_path / "cold_start.duckdb"),
            "access_level": "read_only",
        }
        update_iceberg_view(ro_con, source)
        assert ro_con.execute("SELECT COUNT(*) FROM client_vitals").fetchone()[0] == 0
        assert ro_con.execute("SELECT COUNT(*) FROM client_errors").fetchone()[0] == 0
    finally:
        ro_con.close()


@pytest.mark.usefixtures("skip_view_update")
def test_rum_live_buffer_unlink_race_self_heals(tmp_path, monkeypatch) -> None:
    from backend.core import duckdb as db_mod
    from backend.core.iceberg.view import clear_source_caches, update_iceberg_view
    from backend.repositories import rum as rum_repo

    monkeypatch.setattr(db_mod, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("bucket", "default")))
    vitals_ddl = "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE"
    errors_ddl = "timestamp TIMESTAMPTZ, error_message VARCHAR, error_file VARCHAR, error_line INTEGER, error_col INTEGER, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE"
    buf_vitals_dir = tmp_path / "cache" / "bucket-race" / "buffer" / "client_vitals"
    buf_errors_dir = tmp_path / "cache" / "bucket-race" / "buffer" / "client_errors"
    buf_vitals_dir.mkdir(parents=True, exist_ok=True)
    buf_errors_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.UTC)
    ts1, ts2 = (now - datetime.timedelta(seconds=20)).isoformat(), (now - datetime.timedelta(seconds=10)).isoformat()
    batch1_v, batch2_v = buf_vitals_dir / "batch_001.parquet", buf_vitals_dir / "batch_002.parquet"
    batch1_e, batch2_e = buf_errors_dir / "batch_001.parquet", buf_errors_dir / "batch_002.parquet"

    seed_con = duckdb.connect(":memory:")
    try:
        seed_con.execute(f"CREATE TABLE cv ({vitals_ddl}); CREATE TABLE ce ({errors_ddl});")
        for target, val, path, cid, req, ts in [
            (batch1_v, 1.8, "/surviving", "cid1", "req1", ts1),
            (batch2_v, 2.1, "/unlinked", "cid2", "req2", ts2),
        ]:
            seed_con.execute("DELETE FROM cv")
            seed_con.execute(
                "INSERT INTO cv VALUES (?, 'lcp', ?, 'good', ?, 'Chrome', 'macOS', 'Desktop', ?, ?, 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 30.0)",
                [ts, val, path, cid, req],
            )
            seed_con.execute(f"COPY cv TO '{target}' (FORMAT PARQUET)")
        for target, msg, line, col, path, cid, req, ts in [
            (batch1_e, "ReferenceError: x", 10, 2, "/surviving", "cid3", "req3", ts1),
            (batch2_e, "TypeError: y", 20, 4, "/unlinked", "cid4", "req4", ts2),
        ]:
            seed_con.execute("DELETE FROM ce")
            seed_con.execute(
                "INSERT INTO ce VALUES (?, ?, 'app.js', ?, ?, ?, 'Chrome', 'macOS', 'Desktop', ?, ?, 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 30.0)",
                [ts, msg, line, col, path, cid, req],
            )
            seed_con.execute(f"COPY ce TO '{target}' (FORMAT PARQUET)")
    finally:
        seed_con.close()

    rum_db_path = tmp_path / "svc_race.rum.duckdb"
    source = {
        "name": "svc_race::rum",
        "service_id": "svc_race",
        "bucket": "bucket-race",
        "duckdb_path": str(tmp_path / "svc_race.duckdb"),
        "local_buffer_dir": str(tmp_path / "cache" / "bucket-race" / "buffer"),
        "access_level": "read_only",
    }

    con = duckdb.connect(str(rum_db_path), read_only=False)
    try:
        clear_source_caches("svc_race")
        update_iceberg_view(con, source, force=True)
        batch2_v.unlink()

        start_iso, end_iso = (now - datetime.timedelta(minutes=15)).isoformat(), now.isoformat()
        events = rum_repo.fetch_rum_live_events(
            con, where_sql="timestamp >= ? AND timestamp <= ?", params=[start_iso, end_iso], source=source
        )
        assert "/surviving" in {e[2] for e in events}

        clear_source_caches("svc_race")
        update_iceberg_view(con, source, force=True)
        batch2_e.unlink()

        analytics = rum_repo.fetch_rum_analytics(
            con,
            service_id="svc_race",
            start_time=start_iso,
            end_time=end_iso,
            parsed_filters={},
            where_sql="timestamp >= ? AND timestamp <= ?",
            params=[start_iso, end_iso],
            raw_window_max_s=7200,
            source=source,
        )
        assert analytics["no_data"] is False and analytics["total_beacons"] == 2 and analytics["errors_count"] == 1

        seed_con2 = duckdb.connect(":memory:")
        try:
            seed_con2.execute(f"CREATE TABLE cv ({vitals_ddl}); COPY cv TO '{batch2_v}' (FORMAT PARQUET);")
        finally:
            seed_con2.close()
        clear_source_caches("svc_race")
        update_iceberg_view(con, source, force=True)
        batch2_v.unlink()

        recent_count, last_dt = rum_repo.get_beacon_health(con)
        assert recent_count >= 1 and last_dt is not None
    finally:
        con.close()


@pytest.mark.usefixtures("skip_view_update")
def test_rum_long_window_merges_live_active_hour_tail_without_double_counting(setup_temp_rum_db) -> None:
    from backend.core.rollups.rum import recompute_rum_aggregates

    service_id = "test_service_long_window_live_tail"
    now = datetime.datetime.now(datetime.UTC).replace(minute=30, second=0, microsecond=0)
    three_hours_ago = now - datetime.timedelta(hours=3)
    ten_mins_ago = now - datetime.timedelta(minutes=10)
    five_secs_ago = now - datetime.timedelta(seconds=5)

    initial_beacons = [
        {
            "pathname": "/historical",
            "load_time": 1.2,
            "lcp": 1.5,
            "cls": 0.01,
            "received_at": (three_hours_ago + datetime.timedelta(minutes=i)).isoformat(),
            "req_id": f"hist_{i}",
            "cid": f"cid_hist_{i}",
        }
        for i in range(10)
    ] + [
        {
            "pathname": "/active-early",
            "load_time": 1.4,
            "lcp": 1.8,
            "cls": 0.02,
            "received_at": (ten_mins_ago + datetime.timedelta(minutes=i)).isoformat(),
            "req_id": f"early_{i}",
            "cid": f"cid_early_{i}",
        }
        for i in range(2)
    ]
    _insert_beacons(setup_temp_rum_db, initial_beacons)

    con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
    try:
        recompute_rum_aggregates(con, service_id)
    finally:
        con.close()

    live_beacon = {
        "pathname": "/live-5s-ago",
        "load_time": 2.5,
        "lcp": 2.9,
        "cls": 0.04,
        "received_at": five_secs_ago.isoformat(),
        "req_id": "live_5s_req",
        "cid": "cid_live_5s",
        "exceptions": [{"value": "Error: live_5s_boom", "filename": "live.js", "lineno": 42, "colno": 7}],
    }
    _insert_beacons(setup_temp_rum_db, [live_beacon])

    start_1h, end_iso = iso_z(now - datetime.timedelta(hours=1)), iso_z(now)
    data_1h = client.get(f"/api/services/{service_id}/rum/analytics?start_time={start_1h}&end_time={end_iso}").json()
    assert data_1h["beacon_count"] == 3 and data_1h["error_count"] == 1

    _insert_beacons(setup_temp_rum_db, [live_beacon])
    for hours in (24, 24 * 30):
        start_long = iso_z(now - datetime.timedelta(hours=hours))
        data_long = client.get(
            f"/api/services/{service_id}/rum/analytics?start_time={start_long}&end_time={end_iso}"
        ).json()
        assert data_long["beacon_count"] == 13 and data_long["error_count"] == 1
        assert any(p["path"] == "/live-5s-ago" for p in data_long["worst_pages"])


def _faro_beacon(path: str, lcp: float, cls: float, inp: float, load_time: float) -> dict:
    return {
        "meta": {
            "page": {"url": f"https://example.test{path}"},
            "browser": {"name": "Chrome", "version": "120.0", "mobile": False},
            "os": {"name": "macOS", "version": "14.0"},
        },
        "events": [{"name": "faro.performance.navigation", "attributes": {"pageLoadTime": str(load_time)}}],
        "measurements": [{"type": "web-vitals", "values": {"lcp": lcp, "cls": cls, "inp": inp}}],
    }


@pytest.mark.usefixtures("skip_view_update")
@time_machine.travel("2026-09-22 12:30:00+00:00", tick=False)
def test_faro_shaped_beacons_vitals_exceptions_url_fallback_and_500_masking(setup_temp_rum_db, monkeypatch):
    _insert_beacons(
        setup_temp_rum_db, [_faro_beacon("/home", lcp, lcp / 2, lcp * 50, lcp) for lcp in (1.0, 2.0, 3.0, 4.0)]
    )
    data = client.get("/api/services/test_vitals_faro_shape/rum/analytics").json()
    assert data["vitals"]["lcp"]["p75"] == 3.25 and data["vitals"]["cls"]["p75"] == 1.625
    assert data["vitals"]["inp"]["p75"] == 162 and data["worst_pages"][0]["avg_load_time"] == 2.5

    con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
    try:
        con.execute("DELETE FROM client_vitals; DELETE FROM client_errors;")
    finally:
        con.close()

    exc_beacon = {
        **_faro_beacon("/checkout", 2.0, 0.02, 100, 2.0),
        "exceptions": [
            {
                "value": "TypeError: boom",
                "type": "TypeError",
                "stacktrace": {"frames": [{"filename": "app.js", "lineno": 42, "colno": 7}]},
            }
        ],
    }
    url_fallback_beacon = {
        "meta": {
            "page": {"url": "https://example.test/from-url//nested"},
            "browser": {"name": "Chrome"},
            "os": {"name": "macOS"},
        },
        "events": [{"name": "faro.performance.navigation", "attributes": {"pageLoadTime": "1.5"}}],
        "measurements": [{"type": "web-vitals", "values": {"lcp": 1.5, "cls": 0.02, "inp": 80}}],
    }
    _insert_beacons(setup_temp_rum_db, [exc_beacon] + [url_fallback_beacon for _ in range(10)])
    data2 = client.get("/api/services/test_vitals_faro_exception/rum/analytics").json()
    assert data2["errors"][0]["message"] == "TypeError: boom"
    assert data2["errors"][0]["file"] == "app.js" and data2["errors"][0]["line"] == 42
    assert "/from-url//nested" in [p["path"] for p in data2["worst_pages"]]

    monkeypatch.setattr(
        rum_router,
        "execute_with_stale_view_retry",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("simulated DB corruption mid-query")),
    )
    r_err = client.get("/api/services/test_vitals_mock_fallback/rum/analytics")
    assert r_err.status_code == 500 and r_err.json()["detail"]["message"] == "RUM analytics query failed"
    assert "simulated DB corruption" not in r_err.text


# ── Section 4: RUM JS Assets & Beacon Health ─────────────────────────────────


@pytest.mark.usefixtures("skip_view_update")
def test_get_rum_tracker_and_faro_sdk_js(with_config, monkeypatch):
    svc = "TestRumAssetsSvc"
    with_config[svc] = {"service_id": svc, "rum": {"faro_version": "2.9.0"}}
    fake_js = "console.log('rum tracker code');"
    monkeypatch.setattr("backend.provision.rum_assets.generate_rum_tracker_js", lambda sid: f"{fake_js} /* {sid} */")

    for url, expected_sid in [("/js/rum.js", svc), ("/js/rum.js?service_id=CustomSvc", svc)]:
        r = client.get(url)
        assert r.status_code == 200 and r.headers["content-type"].startswith("application/javascript")
        assert f"{fake_js} /* {expected_sid} */" in r.text

    with_config["CustomSvc"] = {"service_id": "CustomSvc"}
    assert f"{fake_js} /* CustomSvc */" in client.get("/js/rum.js?service_id=CustomSvc").text

    async def mock_fetch(version):
        return f"console.log('faro bundle version {version}');".encode()

    monkeypatch.setattr("backend.core.faro_versions.fetch_faro_bundle", mock_fetch)
    r_sdk = client.get("/js/faro-sdk.js")
    assert r_sdk.status_code == 200 and "console.log('faro bundle version 2.9.0');" in r_sdk.text


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_get_rum_tracker_js_error_does_not_leak_exception_text(with_config, monkeypatch):
    svc = "TestRumAssetsSvc"
    with_config[svc] = {"service_id": svc}
    secret = '*/ </script> alert("x"); \n /etc/secret-path "quoted" \'single\''
    monkeypatch.setattr(
        "backend.provision.rum_assets.generate_rum_tracker_js", lambda sid: (_ for _ in ()).throw(RuntimeError(secret))
    )
    r = client.get("/js/rum.js")
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/javascript")
    for forbidden in ("secret-path", "</script>", "alert", "RuntimeError"):
        assert forbidden not in r.text
    assert "\n" not in r.text.strip() and r.text.count("*/") == 1


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize(
    ("mode", "expected_recent", "expected_complete", "expected_msg_substr"),
    [
        ("recent", 1, True, "Script installed and firing"),
        ("stale", 0, True, "Script installed and firing"),
        ("empty", 0, False, "Waiting for beacons..."),
        ("db_failure", 0, False, "db is locked"),
    ],
)
def test_beacon_health_states(
    setup_temp_rum_db, monkeypatch, mode: str, expected_recent: int, expected_complete: bool, expected_msg_substr: str
):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    monkeypatch.setattr(
        "backend.config.load_config", lambda sid: {"service_id": sid, "rum": {"enabled": True}, "rum_enabled": True}
    )
    now = datetime.datetime.now(datetime.UTC)
    if mode in ("recent", "stale"):
        delta = datetime.timedelta(minutes=5) if mode == "recent" else datetime.timedelta(hours=3)
        _insert_beacons(
            setup_temp_rum_db,
            [
                {
                    "pathname": "/",
                    "load_time": 1.5,
                    "received_at": (now - delta).isoformat(),
                    "cid": "cid1",
                    "req_id": "req1",
                }
            ],
        )
    elif mode == "db_failure":
        monkeypatch.setattr(
            "backend.routers.rum.execute_with_stale_view_retry",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db is locked")),
        )

    r = client.get(f"/api/services/test_beacon_health_{mode}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True and data["recent_beacons"] == expected_recent
    assert data["setup_complete"] is expected_complete and expected_msg_substr in data["message"]


# ── Section 5: Golden Contracts & Token Window Resolution ────────────────────


def _beacons() -> list[dict]:
    specs = [
        ("10:05:00", "/home", "Chrome", "macOS", "Desktop", 1.2, 2100.0, 0.05, 150.0, False),
        ("10:20:00", "/home", "Firefox", "Windows", "Desktop", 2.4, 3400.0, 0.15, 320.0, False),
        ("10:40:00", "/cart", "Chrome", "Android", "Mobile", 3.1, 4800.0, 0.3, 650.0, True),
        ("11:10:00", "/cart", "Safari", "macOS", "Desktop", 0.9, 1800.0, 0.02, 90.0, True),
        ("11:50:00", "/search", "Chrome", "Windows", "Desktop", 1.7, 2600.0, 0.11, 210.0, False),
        ("13:30:00", "/home", "Chrome", "macOS", "Desktop", 1.1, 2000.0, 0.04, 120.0, False),
    ]
    return [
        {
            "received_at": f"2026-08-05T{hms}+00:00",
            "pathname": path,
            "meta": {"browser": browser, "os": os_, "device": device},
            "load_time": load,
            "lcp": lcp,
            "cls": cls,
            "inp": inp,
            "cid": f"cid_{i}",
            "req_id": f"req_{i}",
            **({"exceptions": [{"value": f"Boom {i}", "filename": "app.js"}] * (2 if i == 2 else 1)} if err else {}),
        }
        for i, (hms, path, browser, os_, device, load, lcp, cls, inp, err) in enumerate(specs)
    ]


def _check_golden(name: str, body: Any) -> None:
    if isinstance(body, dict):
        body = {k: v for k, v in body.items() if not k.startswith("_debug")}
    path = GOLDEN_DIR / f"rum_{name}.json"
    rendered = json.dumps(body, indent=2, sort_keys=True) + "\n"
    if os.getenv("UPDATE_GOLDEN") == "1":
        path.write_text(rendered)
    assert path.exists(), f"missing golden {path.name}; run with UPDATE_GOLDEN=1"
    assert json.loads(path.read_text()) == json.loads(rendered)


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize(
    "case",
    [
        "analytics_empty",
        "analytics_raw_long",
        "analytics_raw_short",
        "analytics_filtered",
        "analytics_rollup",
        "live_events",
        "beacon_health_disabled",
    ],
)
def test_rum_golden_contracts(setup_temp_rum_db, case: str) -> None:
    import backend.core.duckdb_pool as duckdb_pool
    from backend.core.rollups.rum import recompute_rum_aggregates

    base = f"/api/services/{_GOLDEN_SERVICE_ID}/rum"
    if case == "analytics_empty":
        _check_golden(case, client.get(f"{base}/analytics?{WINDOW}").json())
    elif case == "analytics_raw_long":
        _insert_beacons(setup_temp_rum_db, _beacons())
        _check_golden(case, client.get(f"{base}/analytics?{WINDOW}").json())
    elif case == "analytics_raw_short":
        _insert_beacons(setup_temp_rum_db, _beacons())
        _check_golden(
            case, client.get(f"{base}/analytics?start_time=2026-08-05T10:00:00Z&end_time=2026-08-05T11:30:00Z").json()
        )
    elif case == "analytics_filtered":
        _insert_beacons(setup_temp_rum_db, _beacons())
        flt = json.dumps({"browser": {"mode": "include", "values": ["Chrome"]}})
        _check_golden(
            case,
            client.get(
                f"{base}/analytics",
                params={"start_time": "2026-08-05T00:00:00Z", "end_time": "2026-08-05T23:59:59Z", "filters": flt},
            ).json(),
        )
    elif case == "analytics_rollup":
        _insert_beacons(setup_temp_rum_db, _beacons())
        duckdb_pool.shutdown_all()
        con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
        try:
            recompute_rum_aggregates(con, _GOLDEN_SERVICE_ID)
            assert con.execute("SELECT COUNT(*) FROM rum_vitals_aggregates").fetchone()[0] > 0
        finally:
            con.close()
        _check_golden(case, client.get(f"{base}/analytics?{WINDOW}").json())
    elif case == "live_events":
        beacons = []
        for i, b in enumerate(_beacons()):
            b_meta = {k: b[k] for k in ("pathname", "meta", "cid", "req_id")}
            beacons.append({**b_meta, "received_at": b["received_at"], "lcp": b["lcp"]})
            if i % 2:
                beacons.append(
                    {
                        **b_meta,
                        "received_at": b["received_at"].replace(":00+", ":30+"),
                        "exceptions": [{"value": f"Err {i}"}],
                    }
                )
        _insert_beacons(setup_temp_rum_db, beacons)
        _check_golden(case, {"events": client.get(f"{base}/live-events?{WINDOW}").json()})
    elif case == "beacon_health_disabled":
        _check_golden(case, client.get(f"{base}/beacon-health").json())


@pytest.fixture
def captured(setup_temp_rum_db):
    calls: list[dict] = []

    def fake(con, **kw):
        calls.append(kw)
        return EMPTY

    with patch("backend.routers.rum.rum_repo.fetch_rum_analytics", fake):
        yield calls


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.parametrize(
    ("params", "expected_bounds"),
    [
        ({"range_token": "24h", "anchor": ANCHOR}, resolve_window("24h", ANCHOR)),
        (
            {
                "range_token": "1h",
                "anchor": ANCHOR,
                "start_time": "2000-01-01T00:00:00Z",
                "end_time": "2000-01-02T00:00:00Z",
            },
            resolve_window("1h", ANCHOR),
        ),
        (
            {"start_time": "2026-08-05T00:00:00Z", "end_time": "2026-08-05T23:59:59Z", "anchor": ANCHOR},
            ("2026-08-05T00:00:00Z", "2026-08-05T23:59:59Z"),
        ),
        (
            {
                "range_token": "bogus",
                "anchor": ANCHOR,
                "start_time": "2026-08-05T00:00:00Z",
                "end_time": "2026-08-05T01:00:00Z",
            },
            ("2026-08-05T00:00:00Z", "2026-08-05T01:00:00Z"),
        ),
    ],
)
def test_token_window_resolution(captured, params: dict, expected_bounds: tuple[str, str]) -> None:
    r = client.get(f"/api/services/{_TOKEN_SERVICE_ID}/rum/analytics", params=params)
    assert r.status_code == 200
    assert (captured[0]["start_time"], captured[0]["end_time"]) == expected_bounds


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_analyst_token_window_is_clamped_to_invite(setup_temp_rum_db, captured) -> None:
    session = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    req = _fake_request(session)
    bounds = get_analyst_time_bounds(typing.cast(typing.Any, req))
    source = _mock_rum_source(setup_temp_rum_db)

    def _run(sess, tb, request_obj) -> None:
        ctx: RequestContext = make_test_request_context(
            service_id=_TOKEN_SERVICE_ID, source=source, session=sess, read_only=True, time_bounds=tb
        )
        asyncio.run(
            rum_router.rum_analytics(
                request=typing.cast(typing.Any, request_obj),
                start_time=None,
                end_time=None,
                filters=None,
                ctx=ctx,
                range_token="30d",
                anchor=None,
            )
        )

    _run(session, bounds, req)
    _run(None, TimeBounds(None, None), _fake_request(None))

    def span_h(c: dict) -> float:
        s, e = parse_iso_utc(c["start_time"]), parse_iso_utc(c["end_time"])
        assert s is not None and e is not None
        return (e - s).total_seconds() / 3600

    assert span_h(captured[0]) <= 1.01
    assert span_h(captured[1]) > 24 * 29


# ── Section 6: Web Vitals, Page Telemetry & UX Events ────────────────────────


@pytest.mark.parametrize(
    ("payload", "expected_status"),
    [
        (
            {
                "id": "v1-1234",
                "name": "LCP",
                "value": 1850.5,
                "rating": "good",
                "pathname": "/dashboard",
                "navigation_type": "navigate",
                "delta": 1850.5,
            },
            200,
        ),
        ({"id": "v1-minimal", "name": "TTFB", "value": 220, "rating": "good"}, 200),
        ({"id": "v1-bad", "name": "NOT_A_REAL_METRIC", "value": 0, "rating": "good"}, 422),
        ({"id": "v1-bad-rating", "name": "CLS", "value": 0.05, "rating": "amazing"}, 422),
    ],
)
def test_web_vitals_payload_validation(payload: dict, expected_status: int):
    r = client.post("/api/web-vitals", json=payload)
    assert r.status_code == expected_status
    if expected_status == 200:
        assert r.json().get("ok") is True


def test_web_vitals_collection_and_page_telemetry(monkeypatch, tmp_path, caplog):
    from backend import config as svcconfig
    from backend.core.metadata import usage_log, usage_log_db

    sink = tmp_path / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)
    monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    r_off = client.post(
        "/api/web-vitals",
        json={"id": "off-1", "name": "LCP", "value": 1200, "rating": "good", "pathname": "/dashboard"},
    )
    assert r_off.status_code == 200 and not sink.exists()

    monkeypatch.setenv("WEB_VITALS_COLLECT", "1")
    r_on = client.post(
        "/api/web-vitals",
        json={
            "id": "on-1",
            "name": "LCP",
            "value": 2412.0,
            "rating": "good",
            "pathname": "/dashboard",
            "navigation_type": "navigate",
            "delta": 2412.0,
        },
    )
    assert r_on.status_code == 200
    rec = json.loads(sink.read_text(encoding="utf-8").strip().splitlines()[0])
    assert rec["name"] == "LCP" and rec["value"] == 2412.0 and rec["cohort"] == "admin" and rec["ts"].endswith("Z")

    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "demo_service")
    usage_log.log_telemetry_query("demo_service", "page-456", "req-789", "duckdb", "SELECT 42", 12.3)

    res_pt = client.get("/api/debug/page-telemetry?page_load_id=page-456")
    assert res_pt.status_code == 200 and res_pt.json()["queries"][0]["sql_query"] == "SELECT 42"

    for payload, expected_status in [
        (
            {
                "event": "column_reordered",
                "pathname": "/sessions",
                "component_id": "Live Sessions",
                "details": {"column_id": "ja4", "from_index": 4, "to_index": 1, "column_count": 9},
            },
            200,
        ),
        ({"event": "filter_cleared"}, 200),
        ({"pathname": "/dashboard"}, 422),
        ({"event": "column_reordered", "typo_field": "ignored"}, 422),
        ({"event": "x" * 81}, 422),
    ]:
        assert client.post("/api/ux-events", json=payload).status_code == expected_status

    with caplog.at_level(logging.INFO, logger="backend.ux_events"):
        r_log = client.post("/api/ux-events", json={"event": "column_reordered"})
    assert r_log.status_code == 200
    matched = [r for r in caplog.records if r.message == "ux_event"]
    assert (
        matched
        and getattr(matched[-1], "ux_cohort", None) == "admin"
        and getattr(matched[-1], "ux_event", None) == "column_reordered"
    )
