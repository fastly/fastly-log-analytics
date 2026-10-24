"""Unit tests for the Admin RUM Ingestion & Configuration endpoints (/admin/rum).

Covers:
  - GET  /api/services/{service_id}/rum/status (admin full view, VCL drift, legacy SHA migration, analyst projection)
  - POST /api/services/{service_id}/rum/settings (capture flags, custom_condition VCL reconcile, rollback on failure)
  - GET  /api/services/{service_id}/rum/versions (pinned vs latest Faro SDK versions)
  - POST /api/services/{service_id}/rum/enable (SSE stream + validation)
  - POST /api/services/{service_id}/rum/disable (SSE stream)
  - POST /api/services/{service_id}/rum/upgrade (SSE stream + registry/version validation)
  - Strict Admin-only RBAC (403 {"error": "admin_only"} for Analyst Path B across all 5 admin/mutation endpoints)
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import typing
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest
import time_machine
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.core import web_vitals_store
from backend.core.request_context import RequestContext
from backend.main import app
from backend.provision.rum_orchestrator_v2 import upgrade_faro_version
from backend.routers import rum as rum_router
from backend.routers.rum import router as _rum_api_router
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.remote_access import TimeBounds, _is_blocked_path, get_analyst_time_bounds
from backend.utils.time_window import resolve_window
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.rum import insert_rum_beacons as _insert
from tests._helpers.rum import insert_rum_beacons as _insert_beacons
from tests._helpers.rum import setup_temp_rum_db

# ── Merged from tests/routers/test_rum_admin.py ──
SVC = "svc-rum-admin-test"


def _parse_sse(text: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("data:"):
            payload = line[len("data:") :].strip()
            if payload:
                events.append(json.loads(payload))
    return events


def _make_app(*, analyst: bool = False) -> FastAPI:
    app = FastAPI()
    if analyst:

        @app.middleware("http")
        async def _inject_analyst(request: Request, call_next):
            request.state.analyst_session = MagicMock(session_id="sess-analyst-rum")
            return await call_next(request)

    app.include_router(_rum_api_router)
    return app


class TestRumStatusEndpoint:
    """GET /api/services/{service_id}/rum/status contract tests."""

    def test_admin_receives_full_status_and_capture_settings(self):
        cfg = {
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
        }
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.routers.rum.rum_vcl_fingerprint", return_value="sha-current"),
        ):
            res = client.get(f"/api/services/{SVC}/rum/status")

        assert res.status_code == 200
        body = res.json()
        assert body == {
            "enabled": True,
            "enabled_at": "2026-10-18T08:00:00Z",
            "deployed_vcl_sha": "sha-current",
            "current_vcl_sha": "sha-current",
            "vcl_drift": False,
            "capture_vitals": True,
            "capture_performance": False,
            "capture_errors": True,
            "capture_events": False,
            "custom_condition": 'req.url.ext != "jpg"',
        }

    def test_admin_detects_vcl_drift_when_sha_differs(self):
        cfg = {
            "rum_enabled": True,
            "rum_enabled_at": "2026-10-18T08:00:00Z",
            "rum_vcl_sha": "sha-old-deployed",
            "rum": {"enabled": True},
        }
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.routers.rum.rum_vcl_fingerprint", return_value="sha-new-template"),
            patch("backend.routers.rum.legacy_rum_vcl_fingerprint", return_value="sha-legacy-unrelated"),
        ):
            res = client.get(f"/api/services/{SVC}/rum/status")

        assert res.status_code == 200
        body = res.json()
        assert body["vcl_drift"] is True
        assert body["deployed_vcl_sha"] == "sha-old-deployed"
        assert body["current_vcl_sha"] == "sha-new-template"

    def test_admin_legacy_vcl_sha_auto_migrates_without_drift(self):
        cfg = {
            "rum_enabled": True,
            "rum_enabled_at": "2026-10-18T08:00:00Z",
            "rum_vcl_sha": "sha-legacy-match",
            "rum": {"enabled": True},
        }
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
            patch("backend.routers.rum.rum_vcl_fingerprint", return_value="sha-new-v2"),
            patch("backend.routers.rum.legacy_rum_vcl_fingerprint", return_value="sha-legacy-match"),
        ):
            res = client.get(f"/api/services/{SVC}/rum/status")

        assert res.status_code == 200
        body = res.json()
        assert body["vcl_drift"] is False
        assert body["deployed_vcl_sha"] == "sha-new-v2"
        mock_save.assert_called_once()

    def test_analyst_receives_minimal_projection_only(self):
        cfg = {
            "rum_enabled": True,
            "rum_enabled_at": "2026-10-18T08:00:00Z",
            "rum_vcl_sha": "sha-secret",
            "rum": {
                "enabled": True,
                "capture_vitals": False,
                "custom_condition": 'req.http.Cookie ~ "internal"',
            },
        }
        client = TestClient(_make_app(analyst=True))
        with patch("backend.config.load_config", return_value=cfg):
            res = client.get(f"/api/services/{SVC}/rum/status")

        assert res.status_code == 200
        assert res.json() == {
            "enabled": True,
            "enabled_at": "2026-10-18T08:00:00Z",
        }


class TestRumSettingsEndpoint:
    """POST /api/services/{service_id}/rum/settings contract tests."""

    def test_update_settings_skips_vcl_reconcile_when_enabled_and_condition_unchanged(self):
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
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
            patch(
                "backend.provision.rum_assets.upload_rum_tracker_js",
                return_value={"skipped": False},
            ) as mock_upload,
            patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile,
        ):
            res = client.post(
                f"/api/services/{SVC}/rum/settings",
                json={
                    "capture_vitals": True,
                    "capture_performance": False,
                    "capture_errors": True,
                    "capture_events": False,
                    "custom_condition": "",
                },
            )

        assert res.status_code == 200
        body = res.json()
        assert body["ok"] is True
        assert body["skipped"] is False
        mock_save.assert_called_once()
        mock_upload.assert_called_once_with(SVC, "tok-stored", overwrite=True)
        mock_reconcile.assert_not_called()

    def test_update_settings_reconciles_vcl_when_custom_condition_changes(self):
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
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config"),
            patch(
                "backend.provision.rum_assets.upload_rum_tracker_js",
                return_value={"skipped": False},
            ),
            patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile,
        ):
            res = client.post(
                f"/api/services/{SVC}/rum/settings",
                json={
                    "capture_vitals": True,
                    "capture_performance": True,
                    "capture_errors": True,
                    "capture_events": True,
                    "custom_condition": 'req.url !~ "^/healthz"',
                },
            )

        assert res.status_code == 200
        assert res.json()["ok"] is True
        mock_reconcile.assert_called_once_with(SVC, "tok-stored", dry_run=False, activate=True)

    def test_update_settings_rolls_back_config_on_fos_upload_failure(self):
        cfg = {
            "fastly_api_key": "tok-stored",
            "rum_enabled": True,
            "rum": {"enabled": True, "capture_vitals": True, "custom_condition": ""},
        }
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
            patch(
                "backend.provision.rum_assets.upload_rum_tracker_js",
                side_effect=RuntimeError("FOS bucket unreachable"),
            ),
        ):
            res = client.post(
                f"/api/services/{SVC}/rum/settings",
                json={
                    "capture_vitals": False,
                    "capture_performance": False,
                    "capture_errors": False,
                    "capture_events": False,
                    "custom_condition": "",
                },
            )

        assert res.status_code == 500
        assert res.json()["detail"]["error"] == "rum_tracker_upload_failed"
        # First call saves updated config, second call rolls back to original_cfg
        assert mock_save.call_count == 2
        rollback_cfg = mock_save.call_args_list[1].args[1]
        assert rollback_cfg["rum"]["capture_vitals"] is True

    def test_update_settings_rolls_back_config_on_vcl_reconcile_failure(self):
        cfg = {
            "fastly_api_key": "tok-stored",
            "rum_enabled": True,
            "rum": {"enabled": True, "capture_vitals": True, "custom_condition": ""},
        }
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save := MagicMock())),
            patch(
                "backend.provision.rum_assets.upload_rum_tracker_js",
                return_value={"skipped": False},
            ),
            patch(
                "backend.provision.declarative.reconciler.reconcile_vcl_state",
                side_effect=RuntimeError("Fastly VCL compile error"),
            ),
        ):
            res = client.post(
                f"/api/services/{SVC}/rum/settings",
                json={
                    "capture_vitals": True,
                    "capture_performance": True,
                    "capture_errors": True,
                    "capture_events": True,
                    "custom_condition": 'req.http.X-Test == "1"',
                },
            )

        assert res.status_code == 500
        assert res.json()["detail"]["error"] == "rum_reconcile_failed"
        assert mock_save.call_count == 2
        rollback_cfg = mock_save.call_args_list[1].args[1]
        assert rollback_cfg["rum"]["custom_condition"] == ""

    def test_update_settings_404_when_service_missing(self):
        client = TestClient(_make_app(analyst=False))
        with patch("backend.config.load_config", return_value=None):
            res = client.post(
                f"/api/services/{SVC}/rum/settings",
                json={"capture_vitals": True},
            )
        assert res.status_code == 404


class TestRumVersionsAndSseEndpoints:
    """GET /rum/versions and POST /rum/{enable,disable,upgrade} tests."""

    def test_versions_reports_pinned_and_latest_with_update_available(self):
        cfg = {"rum": {"faro_version": "2.2.0"}}
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.routers.rum.get_faro_versions_cached",
                new=AsyncMock(return_value=["2.3.1", "2.2.0", "2.1.0"]),
            ),
        ):
            res = client.get(f"/api/services/{SVC}/rum/versions")

        assert res.status_code == 200
        assert res.json() == {
            "available": ["2.3.1", "2.2.0", "2.1.0"],
            "current": "2.2.0",
            "latest": "2.3.1",
            "update_available": True,
        }

    def test_enable_rum_sse_stream_emits_done(self):
        initial_cfg = {"fastly_api_key": "tok-stored", "rum_enabled": False}
        enabled_cfg = {
            "fastly_api_key": "tok-stored",
            "rum_enabled": True,
            "rum_enabled_at": "2026-10-18T10:00:00Z",
            "rum": {"enabled": True},
        }
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", side_effect=[initial_cfg, enabled_cfg]),
            patch(
                "backend.routers.rum.fetch_available_faro_versions",
                new=AsyncMock(return_value=["2.3.1"]),
            ),
            patch(
                "backend.routers.rum.run_with_events",
                return_value=iter([{"type": "step", "step": "vcl", "status": "ok"}]),
            ),
            patch("backend.cron.scheduler.get_scheduler"),
        ):
            res = client.post(f"/api/services/{SVC}/rum/enable", json={"activate": True})

        assert res.status_code == 200
        events = _parse_sse(res.text)
        assert any(e.get("type") == "done" and e.get("rum", {}).get("enabled") is True for e in events)

    def test_disable_rum_sse_stream_emits_done(self):
        disabled_cfg = {"fastly_api_key": "tok-stored", "rum_enabled": False}
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=disabled_cfg),
            patch(
                "backend.routers.rum.run_with_events",
                return_value=iter([{"type": "step", "step": "vcl", "status": "ok"}]),
            ),
            patch("backend.cron.scheduler.get_scheduler"),
        ):
            res = client.post(f"/api/services/{SVC}/rum/disable", json={"activate": True})

        assert res.status_code == 200
        events = _parse_sse(res.text)
        assert any(e.get("type") == "done" and e.get("rum", {}).get("enabled") is False for e in events)


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
        self,
        method: str,
        suffix: str,
        payload: dict[str, Any] | None,
    ):
        path = f"/api/services/{SVC}/rum/{suffix}"
        # 1. Middleware path gate blocks the path
        assert _is_blocked_path(path) is True

        # 2. Route-level require_admin dependency also blocks with 403 admin_only
        client = TestClient(_make_app(analyst=True))
        if method == "GET":
            res = client.get(path)
        else:
            res = client.post(path, json=payload)

        assert res.status_code == 403
        assert res.json()["detail"]["error"] == "admin_only"


# ── Merged from tests/routers/test_rum_analytics.py ──
@pytest.fixture
def skip_view_update(request):
    if request.node.name in (
        "test_rum_analytics_cold_start_readonly",
        "test_rum_live_buffer_unlink_race_self_heals",
    ):
        yield None
    else:
        with patch("backend.core.iceberg.view.update_iceberg_view") as mock:
            yield mock


__all__ = ["setup_temp_rum_db"]

client = TestClient(app)


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_fallback(setup_temp_rum_db) -> None:
    # Target service id that has no RUM data yet, should return "no data" response
    response = client.get("/api/services/svc-test-rum-fallback/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False
    assert data["no_data"] is True
    assert "beacon_count" in data
    assert data["beacon_count"] == 0
    assert "message" in data
    assert "vitals" in data
    assert "worst_pages" in data
    assert "errors" in data
    assert "trends" in data
    assert "environments" in data

    # Verify empty collections
    assert data["worst_pages"] == []
    assert data["errors"] == []
    assert data["trends"]["lcp"] == []


@pytest.mark.usefixtures("skip_view_update")
def test_rum_live_events(setup_temp_rum_db) -> None:
    # 1. When empty, should return a clean empty list
    response = client.get("/api/services/svc-test-rum-fallback/rum/live-events")
    assert response.status_code == 200
    events = response.json()
    assert isinstance(events, list)
    assert len(events) == 0

    # 2. Insert a real test beacon
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

    # 3. Verify they are now returned in the ticker
    response = client.get("/api/services/svc-test-rum-fallback/rum/live-events")
    assert response.status_code == 200
    events = response.json()
    assert isinstance(events, list)
    assert len(events) == 4
    assert events[0]["path"] == "/live-test"
    assert events[0]["browser"] == "Chrome"
    assert events[0]["os"] == "macOS"


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_real_data(setup_temp_rum_db) -> None:
    service_id = "test_service_real_data"

    # Populate with 12 distinct mock beacons (>= 10) to trigger real aggregation path
    test_beacons = [
        {
            "pathname": "/pricing",
            "load_time": 2.1,
            "lcp": 2.3,
            "cls": 0.02,
            "inp": 120,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            "exceptions": [{"value": "TypeError: null is not an object", "filename": "main.js"}],
            "req_id": "",
        },
        {
            "pathname": "/",
            "load_time": 1.4,
            "lcp": 1.6,
            "cls": 0.01,
            "inp": 80,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/pricing",
            "load_time": 2.5,
            "lcp": 2.8,
            "cls": 0.03,
            "inp": 150,
            "meta": {"browser": "Safari", "os": "iOS", "device": "Mobile"},
        },
        {
            "pathname": "/docs",
            "load_time": 1.8,
            "lcp": 1.9,
            "cls": 0.05,
            "inp": 90,
            "meta": {"browser": "Firefox", "os": "Linux", "device": "Desktop"},
        },
        {
            "pathname": "/",
            "load_time": 1.2,
            "lcp": 1.4,
            "cls": 0.01,
            "inp": 70,
            "meta": {"browser": "Chrome", "os": "Windows", "device": "Desktop"},
        },
        {
            "pathname": "/checkout",
            "load_time": 3.2,
            "lcp": 3.5,
            "cls": 0.12,
            "inp": 220,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            "exceptions": [{"value": "TypeError: null is not an object", "filename": "main.js"}],
        },
        {
            "pathname": "/pricing",
            "load_time": 2.0,
            "lcp": 2.2,
            "cls": 0.02,
            "inp": 110,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/",
            "load_time": 1.5,
            "lcp": 1.7,
            "cls": 0.02,
            "inp": 85,
            "meta": {"browser": "Safari", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/docs",
            "load_time": 1.9,
            "lcp": 2.1,
            "cls": 0.04,
            "inp": 100,
            "meta": {"browser": "Chrome", "os": "Android", "device": "Mobile"},
        },
        {
            "pathname": "/",
            "load_time": 1.3,
            "lcp": 1.5,
            "cls": 0.01,
            "inp": 75,
            "meta": {"browser": "Edge", "os": "Windows", "device": "Desktop"},
        },
        {
            "pathname": "/pricing",
            "load_time": 2.2,
            "lcp": 2.4,
            "cls": 0.02,
            "inp": 130,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/checkout",
            "load_time": 3.0,
            "lcp": 3.2,
            "cls": 0.10,
            "inp": 200,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            "exceptions": [{"value": "ReferenceError: x is not defined", "filename": "checkout.js"}],
        },
    ]

    _insert_beacons(setup_temp_rum_db, test_beacons)

    # Hit analytics endpoint
    response = client.get(f"/api/services/{service_id}/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    print("DEBUG DATA:", json.dumps(data, indent=2))
    assert data["is_mock"] is False

    # Verify vitals calculations
    vitals = data["vitals"]
    assert "lcp" in vitals
    assert "cls" in vitals
    assert "inp" in vitals

    # Verify LCP distribution calculation
    # Good limit is <= 2.5s, Poor is > 4.0s. All LCPs in mock are <= 3.5s, so good/needs-improvement
    assert vitals["lcp"]["distribution"]["good"] > 0
    assert vitals["lcp"]["distribution"]["poor"] == 0

    # Verify browser, OS and device aggregates
    envs = data["environments"]
    assert envs["browsers"].get("Chrome", 0) == 8
    assert envs["os"].get("macOS", 0) == 7
    assert envs["devices"].get("Desktop", 0) == 10

    # Verify error grouping (2 main.js errors, 1 checkout.js error)
    errors = data["errors"]
    assert len(errors) == 2
    assert errors[0]["count"] == 2
    assert errors[1]["count"] == 1

    # Hit live-events endpoint
    response_live = client.get(f"/api/services/{service_id}/rum/live-events")
    assert response_live.status_code == 200
    events = response_live.json()
    assert len(events) >= 12


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_recent_window_serves_raw_without_in_request_recompute(setup_temp_rum_db) -> None:
    """A short/recent RUM window (e.g. the live 15m view) must serve beacon
    counts directly from the already-attached read connection and must NOT
    trigger a synchronous in-request ``recompute_rum_aggregates`` — that opens a
    second write connection and competes for the process-wide DuckLake attach
    lock, stalling the page under verify-phase load. Mirrors the request-log
    'recent data = direct scan' principle."""
    service_id = "test_service_recent_raw"

    now = datetime.datetime.now(datetime.UTC)
    test_beacons = []
    for i in range(12):
        received = (now - datetime.timedelta(minutes=2 + i)).isoformat()
        test_beacons.append(
            {
                "pathname": f"/recent_{i}",
                "load_time": 1.4,
                "lcp": 1.8,
                "cls": 0.02,
                "received_at": received,
                "req_id": f"rid_{i}",
            }
        )
    _insert_beacons(setup_temp_rum_db, test_beacons)

    start = (now - datetime.timedelta(minutes=30)).isoformat()
    end = now.isoformat()

    with patch("backend.core.rollups.rum.recompute_rum_aggregates") as mock_recompute:
        response = client.get(f"/api/services/{service_id}/rum/analytics?start_time={start}&end_time={end}")

    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False
    assert data["beacon_count"] > 0
    mock_recompute.assert_not_called()


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_long_window_does_not_recompute_in_request(setup_temp_rum_db) -> None:
    """A longer RUM window (e.g. the verifier's 24h check) must also serve
    without a synchronous in-request ``recompute_rum_aggregates``. Like the
    request-log dashboard, the read path serves from cron-built rollups when they
    exist and otherwise scans the raw tables directly — it never opens a second
    read-write DuckLake connection. In-request recompute competes for the
    process-wide attach lock and, under verify-phase concurrency, wedged the
    entire /api surface (120s pool-timeout at ``SELECT MAX(timestamp) FROM
    client_vitals``). The rum_commit cron owns all recompute."""
    service_id = "test_service_long_window"

    now = datetime.datetime.now(datetime.UTC)
    test_beacons = []
    for i in range(12):
        received = (now - datetime.timedelta(hours=1, minutes=i)).isoformat()
        test_beacons.append(
            {
                "pathname": f"/p_{i}",
                "load_time": 1.4,
                "lcp": 1.8,
                "cls": 0.02,
                "received_at": received,
                "req_id": f"rid_{i}",
            }
        )
    _insert_beacons(setup_temp_rum_db, test_beacons)

    start = iso_z(now - datetime.timedelta(hours=24))
    end = iso_z(now)

    # Spy on get_connection: the read handler must never open a second
    # read-write connection (read_only=False). The pooled read path uses
    # checkout_connection / get_connection(read_only=True); only the removed
    # in-request recompute blocks opened a write connection, which is what
    # stalled on the attach lock under load.
    import backend.core.duckdb as _duckdb_mod

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
    assert data["is_mock"] is False
    assert data["beacon_count"] > 0
    mock_recompute.assert_not_called()
    assert write_conn_calls == [], (
        f"RUM read handler opened {len(write_conn_calls)} read-write connection(s); "
        "reads must be pure-read and let the rum_commit cron own recompute"
    )


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_date_filtering(setup_temp_rum_db) -> None:
    service_id = "test_service_date_filter"

    test_beacons = []
    for i in range(20):
        b = {"pathname": f"/page_{i}", "load_time": 1.5}
        if i < 10:
            b["received_at"] = "2026-08-05T10:00:00+00:00"
        else:
            b["received_at"] = "2026-08-05T12:00:00+00:00"
        test_beacons.append(b)

    _insert_beacons(setup_temp_rum_db, test_beacons)

    response = client.get(f"/api/services/{service_id}/rum/analytics?start_time=2026-08-05T11:00:00+00:00")
    assert response.status_code == 200
    data = response.json()
    assert not data["is_mock"]

    paths = [p["path"] for p in data["worst_pages"]]
    for p in paths:
        assert int(p.split("_")[1]) >= 10

    response = client.get(f"/api/services/{service_id}/rum/analytics?end_time=2026-08-05T11:00:00+00:00")
    data = response.json()
    assert not data["is_mock"]
    paths = [p["path"] for p in data["worst_pages"]]
    for p in paths:
        assert int(p.split("_")[1]) < 10


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_total_beacons_does_not_double_count_shared_distinct_id(setup_temp_rum_db) -> None:
    """A single physical beacon event (one cid/timestamp, empty req_id -> one
    distinct_id) can legitimately emit both a vitals metric row and an
    interaction (event_*) row at once. beacon_count must count that as ONE
    beacon, matching the header's single unified COUNT(DISTINCT distinct_id)
    (backend/routers/bootstrap.py) — not sum separate pageviews/interactions/
    errors_count distinct-id partitions, which double-counts it. Reproduces
    the live Local Standard 30d-consistency failure from the 2026-09-30
    canonical deploy run (Page RUM count exceeded header lifetime count)."""
    service_id = "test_service_shared_distinct_id"
    rum_db_path = setup_temp_rum_db

    shared_ts = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
    other_ts = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=6)

    con = duckdb.connect(str(rum_db_path), read_only=False)
    try:
        # Beacon A: one physical event (cid="shared", empty req_id, same
        # timestamp) emitting BOTH a vitals metric and an interaction event.
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
        # Beacon B: a separate, unrelated error beacon.
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

    response = client.get(f"/api/services/{service_id}/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert not data["is_mock"]
    # Two physical beacons total: the shared vitals+interaction event, and
    # the separate error. Must not be inflated to 3 by double-counting the
    # shared distinct_id across the pageviews/interactions partitions.
    assert data["beacon_count"] == 2
    assert data["pageview_count"] == 1
    assert data["error_count"] == 1


@pytest.mark.usefixtures("skip_view_update")
def test_rum_status_endpoint(setup_temp_rum_db) -> None:
    service_id = "svc-test-rum-status"
    response = client.get(f"/api/services/{service_id}/rum/status")
    assert response.status_code == 200
    data = response.json()
    assert "enabled" in data
    assert isinstance(data["enabled"], bool)


@pytest.mark.usefixtures("skip_view_update")
def test_rum_beacon_health_endpoint(setup_temp_rum_db) -> None:
    service_id = "svc-test-rum-health"
    response = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert response.status_code == 200
    data = response.json()
    assert "enabled" in data
    assert "recent_beacons" in data
    assert "last_beacon_time" in data
    assert "setup_complete" in data


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_with_filters(setup_temp_rum_db) -> None:
    service_id = "test_service_with_filters"

    # Insert 15 beacons to satisfy the minimum requirements
    test_beacons = []
    for i in range(15):
        b = {
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
        test_beacons.append(b)

    _insert_beacons(setup_temp_rum_db, test_beacons)

    # Filter browser Chrome
    filters = json.dumps({"browser": {"mode": "include", "values": ["Chrome"]}})
    response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert response.status_code == 200
    data = response.json()
    assert not data["is_mock"]
    assert data["environments"]["browsers"] == {"Chrome": 8}

    # Filter browser Safari
    filters = json.dumps({"browser": {"mode": "include", "values": ["Safari"]}})
    response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert response.status_code == 200
    data = response.json()
    assert not data["is_mock"]
    assert data["environments"]["browsers"] == {"Safari": 7}

    # Filter non-existent browser should return zero matching count and empty/no_data=False dictionary
    filters = json.dumps({"browser": {"mode": "include", "values": ["Firefox"]}})
    response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert response.status_code == 200
    data = response.json()
    assert data["beacon_count"] == 0
    assert data["is_mock"] is False
    assert data["no_data"] is False


def _fake_request(analyst_session: object | None) -> SimpleNamespace:
    """Minimal stand-in — only ``request.state.analyst_session`` is read by
    ``get_analyst_time_bounds``/``clamp_or_400``. Same idiom as
    ``test_session_scoring_router.py``'s ``_fake_request``."""
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _analyst_session(window_hours: int = 1) -> SimpleNamespace:
    return SimpleNamespace(query_window_hours=window_hours, query_start_time=None, query_end_time=None)


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_analyst_request_outside_invite_window_is_clamped(setup_temp_rum_db) -> None:
    """An analyst scoped to a 1-hour invite who asks for start_time way in
    the past must not see beacons outside that window — even though the
    beacon itself belongs to their own service."""
    service_id = "test_rum_analytics_clamp_analyst"

    old_ts = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=30)).isoformat()
    recent_ts = datetime.datetime.now(datetime.UTC).isoformat()

    test_beacons = [
        {"pathname": "/thirty-days-old", "load_time": 1.5, "received_at": old_ts},
        {"pathname": "/just-now", "load_time": 1.5, "received_at": recent_ts},
    ]
    _insert_beacons(setup_temp_rum_db, test_beacons)

    # Analyst asking for a range far wider than their 1h invite window.
    analyst_sess = _analyst_session(window_hours=1)
    analyst_req = _fake_request(analyst_sess)
    import typing

    from tests.conftest import make_test_request_context

    analyst_time_bounds = get_analyst_time_bounds(typing.cast(typing.Any, analyst_req))

    analyst_ctx = make_test_request_context(
        service_id=service_id,
        source={
            "name": "test_service",
            "service_id": "test_service",
            "duckdb_path": str(setup_temp_rum_db).replace(".rum.duckdb", ".duckdb"),
            "access_level": "read_write",
            "endpoint": "localhost",
            "access_key_id": "mock",
            "secret_access_key": "mock",
            "region": "mock",
        },
        session=analyst_sess,
        read_only=True,
        time_bounds=analyst_time_bounds,
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
    paths = [p["path"] for p in result["worst_pages"]]
    assert "/thirty-days-old" not in paths, "analyst invite-window clamp did not apply"

    from backend.utils.remote_access import TimeBounds

    # Negative control: admin (no analyst_session) with the same
    # far-in-the-past start_time is NOT clamped — sees the old beacon.
    admin_ctx = make_test_request_context(
        service_id=service_id,
        source={
            "name": "test_service",
            "service_id": "test_service",
            "duckdb_path": str(setup_temp_rum_db).replace(".rum.duckdb", ".duckdb"),
            "access_level": "read_write",
            "endpoint": "localhost",
            "access_key_id": "mock",
            "secret_access_key": "mock",
            "region": "mock",
        },
        session=None,
        read_only=True,
        time_bounds=TimeBounds(None, None),
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
    admin_paths = [p["path"] for p in admin_result["worst_pages"]]
    assert "/thirty-days-old" in admin_paths


@pytest.mark.usefixtures("skip_view_update")
def test_rum_analytics_cold_start_readonly(tmp_path, monkeypatch):
    """Verifies that rum_analytics handles a cold start gracefully on a read-only DuckDB connection."""
    # Create a fresh database that has NO tables or views
    temp_db_path = tmp_path / "cold_start.rum.duckdb"
    con = duckdb.connect(str(temp_db_path))
    con.close()

    # Open the connection in read_only mode to simulate a read-only database environment
    ro_con = duckdb.connect(str(temp_db_path), read_only=True)

    try:
        from backend.core.iceberg.view import update_iceberg_view

        source = {
            "name": "cold_start::rum",
            "service_id": "cold_start",
            "duckdb_path": str(tmp_path / "cold_start.duckdb"),
            "access_level": "read_only",
        }

        # This should execute successfully and create the empty temporary views
        # client_vitals and client_errors in the read-only catalog
        update_iceberg_view(ro_con, source)

        # Confirm the views are created successfully and can be queried
        res_vitals = ro_con.execute("SELECT COUNT(*) FROM client_vitals").fetchone()
        res_errors = ro_con.execute("SELECT COUNT(*) FROM client_errors").fetchone()

        assert res_vitals[0] == 0
        assert res_errors[0] == 0

    finally:
        ro_con.close()


@pytest.mark.usefixtures("skip_view_update")
def test_rum_live_buffer_unlink_race_self_heals(tmp_path, monkeypatch) -> None:
    """When rum_commit drains and unlinks a batch_*.parquet buffer file after
    client_vitals / client_errors views were bound to an explicit file list,
    fetch_rum_live_events, fetch_rum_analytics, and get_beacon_health must
    catch the stale-view IO Error, re-bind the views against the surviving
    files, and return the surviving rows instead of dropping live data or
    leaving leaked temp tables on the connection."""
    from backend.core import duckdb as db_mod
    from backend.core.iceberg.view import clear_source_caches, update_iceberg_view
    from backend.repositories import rum as rum_repo

    monkeypatch.setattr(db_mod, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("bucket", "default")))

    vitals_ddl = """
        timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR,
        pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR,
        city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE
    """
    errors_ddl = """
        timestamp TIMESTAMPTZ, error_message VARCHAR, error_file VARCHAR, error_line INTEGER,
        error_col INTEGER, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR,
        cid VARCHAR, req_id VARCHAR, city VARCHAR, region VARCHAR, country VARCHAR,
        pop VARCHAR, tls VARCHAR, ttfb DOUBLE
    """

    buf_vitals_dir = tmp_path / "cache" / "bucket-race" / "buffer" / "client_vitals"
    buf_errors_dir = tmp_path / "cache" / "bucket-race" / "buffer" / "client_errors"
    buf_vitals_dir.mkdir(parents=True, exist_ok=True)
    buf_errors_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.UTC)
    ts1 = (now - datetime.timedelta(seconds=20)).isoformat()
    ts2 = (now - datetime.timedelta(seconds=10)).isoformat()

    batch1_v = buf_vitals_dir / "batch_001.parquet"
    batch2_v = buf_vitals_dir / "batch_002.parquet"
    batch1_e = buf_errors_dir / "batch_001.parquet"
    batch2_e = buf_errors_dir / "batch_002.parquet"

    seed_con = duckdb.connect(":memory:")
    try:
        seed_con.execute(f"CREATE TABLE cv ({vitals_ddl})")
        seed_con.execute(
            "INSERT INTO cv VALUES (?, 'lcp', 1.8, 'good', '/surviving', 'Chrome', 'macOS', 'Desktop', 'cid1', 'req1', 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 30.0)",
            [ts1],
        )
        seed_con.execute(f"COPY cv TO '{batch1_v}' (FORMAT PARQUET)")
        seed_con.execute("DELETE FROM cv")
        seed_con.execute(
            "INSERT INTO cv VALUES (?, 'lcp', 2.1, 'good', '/unlinked', 'Chrome', 'macOS', 'Desktop', 'cid2', 'req2', 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 35.0)",
            [ts2],
        )
        seed_con.execute(f"COPY cv TO '{batch2_v}' (FORMAT PARQUET)")

        seed_con.execute(f"CREATE TABLE ce ({errors_ddl})")
        seed_con.execute(
            "INSERT INTO ce VALUES (?, 'ReferenceError: x', 'app.js', 10, 2, '/surviving', 'Chrome', 'macOS', 'Desktop', 'cid3', 'req3', 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 30.0)",
            [ts1],
        )
        seed_con.execute(f"COPY ce TO '{batch1_e}' (FORMAT PARQUET)")
        seed_con.execute("DELETE FROM ce")
        seed_con.execute(
            "INSERT INTO ce VALUES (?, 'TypeError: y', 'app.js', 20, 4, '/unlinked', 'Chrome', 'macOS', 'Desktop', 'cid4', 'req4', 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 35.0)",
            [ts2],
        )
        seed_con.execute(f"COPY ce TO '{batch2_e}' (FORMAT PARQUET)")
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

        # Simulate concurrent rum_commit draining and unlinking batch_002.parquet
        # AFTER the view was already bound to [batch_001.parquet, batch_002.parquet]
        batch2_v.unlink()

        # 1. fetch_rum_live_events must self-heal and return the surviving vitals + error rows
        events = rum_repo.fetch_rum_live_events(
            con,
            where_sql="timestamp >= ? AND timestamp <= ?",
            params=[(now - datetime.timedelta(minutes=15)).isoformat(), now.isoformat()],
            source=source,
        )
        assert len(events) >= 2
        paths = {e[2] for e in events}
        assert "/surviving" in paths

        # 2. Simulate batch2_e being unlinked AFTER t_client_vitals succeeds inside fetch_rum_analytics
        # First re-create batch2_v so client_vitals has 2 files, bind view, then unlink batch2_e
        # so t_client_vitals succeeds and t_client_errors hits the missing file!
        clear_source_caches("svc_race")
        update_iceberg_view(con, source, force=True)
        batch2_e.unlink()

        start_iso = (now - datetime.timedelta(minutes=15)).isoformat()
        end_iso = now.isoformat()
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
        assert analytics["no_data"] is False
        assert analytics["total_beacons"] == 2
        assert analytics["errors_count"] == 1

        # 3. Simulate another unlink before get_beacon_health (even without passing source!)
        seed_con2 = duckdb.connect(":memory:")
        try:
            seed_con2.execute(f"CREATE TABLE cv ({vitals_ddl})")
            seed_con2.execute(f"COPY cv TO '{batch2_v}' (FORMAT PARQUET)")
        finally:
            seed_con2.close()
        clear_source_caches("svc_race")
        update_iceberg_view(con, source, force=True)
        batch2_v.unlink()

        recent_count, last_dt = rum_repo.get_beacon_health(con)
        assert recent_count >= 1
        assert last_dt is not None
    finally:
        con.close()


@pytest.mark.usefixtures("skip_view_update")
def test_rum_long_window_merges_live_active_hour_tail_without_double_counting(setup_temp_rum_db) -> None:
    """A beacon that landed 5 seconds ago must appear immediately in both 1h
    (raw path) and 24h / 30d (rollup path) RUM analytics queries without waiting
    for recompute_rum_aggregates, while neither double-counting beacons already
    rolled up in the active hour nor double-counting duplicate rows with the
    same composite natural key."""
    from backend.core.rollups.rum import recompute_rum_aggregates

    service_id = "test_service_long_window_live_tail"
    now = datetime.datetime.now(datetime.UTC).replace(minute=30, second=0, microsecond=0)
    three_hours_ago = now - datetime.timedelta(hours=3)
    ten_mins_ago = now - datetime.timedelta(minutes=10)
    five_secs_ago = now - datetime.timedelta(seconds=5)

    # 1. Seed 10 beacons 3 hours ago + 2 beacons 10 minutes ago (same active hour as `now`)
    initial_beacons = []
    for i in range(10):
        initial_beacons.append(
            {
                "pathname": "/historical",
                "load_time": 1.2,
                "lcp": 1.5,
                "cls": 0.01,
                "received_at": (three_hours_ago + datetime.timedelta(minutes=i)).isoformat(),
                "req_id": f"hist_{i}",
                "cid": f"cid_hist_{i}",
            }
        )
    for i in range(2):
        initial_beacons.append(
            {
                "pathname": "/active-early",
                "load_time": 1.4,
                "lcp": 1.8,
                "cls": 0.02,
                "received_at": (ten_mins_ago + datetime.timedelta(minutes=i)).isoformat(),
                "req_id": f"early_{i}",
                "cid": f"cid_early_{i}",
            }
        )
    _insert_beacons(setup_temp_rum_db, initial_beacons)

    # 2. Run rollup cron once so both `three_hours_ago` and `now`'s hour exist in rum_*_aggregates
    con = duckdb.connect(str(setup_temp_rum_db), read_only=False)
    try:
        recompute_rum_aggregates(con, service_id)
    finally:
        con.close()

    # 3. Land a new beacon 5 seconds ago WITHOUT re-running rollups.
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

    # 4. Query 1h window (raw scan) — newly landed beacon appears immediately
    start_1h = iso_z(now - datetime.timedelta(hours=1))
    end_iso = iso_z(now)
    resp_1h = client.get(f"/api/services/{service_id}/rum/analytics?start_time={start_1h}&end_time={end_iso}")
    assert resp_1h.status_code == 200
    data_1h = resp_1h.json()
    assert data_1h["beacon_count"] == 3
    assert data_1h["error_count"] == 1
    assert any(p["path"] == "/live-5s-ago" for p in data_1h["worst_pages"])
    assert any(e["message"] == "Error: live_5s_boom" and e["count"] == 1 for e in data_1h["errors"])

    # Insert an exact natural-key duplicate row simulating uncommitted buffer +
    # committed table overlap during the long-window tail merge.
    _insert_beacons(setup_temp_rum_db, [live_beacon])

    # 5. Query 24h and 30d windows (rollup + deduplicated active-hour tail merge)
    for hours in (24, 24 * 30):
        start_long = iso_z(now - datetime.timedelta(hours=hours))
        resp_long = client.get(f"/api/services/{service_id}/rum/analytics?start_time={start_long}&end_time={end_iso}")
        assert resp_long.status_code == 200
        data_long = resp_long.json()
        # 10 historical + 2 early-active + 1 deduplicated live-5s-ago = 13 total beacons
        assert data_long["beacon_count"] == 13
        assert data_long["error_count"] == 1
        assert any(p["path"] == "/live-5s-ago" for p in data_long["worst_pages"])
        assert any(e["message"] == "Error: live_5s_boom" and e["count"] == 1 for e in data_long["errors"])


_SVC_rum_analytics = "TestRumAssetsSvc"


@pytest.mark.usefixtures("skip_view_update")
def test_get_rum_tracker_js(with_config, monkeypatch):
    # Set up config
    with_config[_SVC_rum_analytics] = {"service_id": _SVC_rum_analytics, "rum": {"faro_version": "2.9.0"}}

    # Mock generate_rum_tracker_js
    fake_js = "console.log('rum tracker code');"
    monkeypatch.setattr("backend.provision.rum_assets.generate_rum_tracker_js", lambda sid: f"{fake_js} /* {sid} */")

    # 1. No headers/cookies -> defaults to active service
    r = client.get("/js/rum.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert f"{fake_js} /* {_SVC_rum_analytics} */" in r.text

    # 2. Service ID in query param
    r = client.get("/js/rum.js?service_id=CustomSvc")
    # CustomSvc not loaded in with_config, so it should fall back to first active
    assert r.status_code == 200
    assert f"{fake_js} /* {_SVC_rum_analytics} */" in r.text

    # With CustomSvc in config
    with_config["CustomSvc"] = {"service_id": "CustomSvc"}
    r = client.get("/js/rum.js?service_id=CustomSvc")
    assert r.status_code == 200
    assert f"{fake_js} /* CustomSvc */" in r.text


@pytest.mark.usefixtures("skip_view_update")
def test_get_faro_sdk_js(with_config, monkeypatch):
    with_config[_SVC_rum_analytics] = {"service_id": _SVC_rum_analytics, "rum": {"faro_version": "2.9.0"}}

    async def mock_fetch(version):
        return f"console.log('faro bundle version {version}');".encode()

    monkeypatch.setattr("backend.core.faro_versions.fetch_faro_bundle", mock_fetch)

    r = client.get("/js/faro-sdk.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert "console.log('faro bundle version 2.9.0');" in r.text


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_get_rum_tracker_js_error_does_not_leak_exception_text(with_config, monkeypatch):
    with_config[_SVC_rum_analytics] = {"service_id": _SVC_rum_analytics}
    secret = '*/ </script> alert("x"); \n /etc/secret-path "quoted" \'single\''

    def boom(sid):
        raise RuntimeError(secret)

    monkeypatch.setattr("backend.provision.rum_assets.generate_rum_tracker_js", boom)
    r = client.get("/js/rum.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert "secret-path" not in r.text
    assert "</script>" not in r.text
    assert "alert" not in r.text
    assert "RuntimeError" not in r.text
    assert "\n" not in r.text.strip()
    assert r.text.count("*/") == 1


@pytest.fixture
def _mock_rum_enabled_config(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda sid: {
            "service_id": sid,
            "rum": {"enabled": True},
            "rum_enabled": True,
        },
    )


@pytest.mark.usefixtures("skip_view_update")
def test_enabled_with_recent_beacons_reports_fire_rate_and_setup_complete(setup_temp_rum_db, _mock_rum_enabled_config):
    service_id = "test_beacon_health_recent"
    recent = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)

    _insert_beacons(
        setup_temp_rum_db,
        [{"pathname": "/", "load_time": 1.5, "received_at": recent.isoformat(), "cid": "cid1", "req_id": "req1"}],
    )

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 1
    assert data["beacon_fire_rate"] == 1
    assert data["setup_complete"] is True
    assert data["message"] == "Script installed and firing"
    assert data["last_beacon_time"] is not None


@pytest.mark.usefixtures("skip_view_update")
def test_enabled_with_no_recent_beacons_reports_waiting(setup_temp_rum_db, _mock_rum_enabled_config):
    service_id = "test_beacon_health_none_recent"
    stale = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=3)

    _insert_beacons(
        setup_temp_rum_db,
        [{"pathname": "/", "load_time": 1.5, "received_at": stale.isoformat(), "cid": "cid1", "req_id": "req1"}],
    )

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 0
    assert data["setup_complete"] is True
    assert data["message"] == "Script installed and firing"
    assert data["last_beacon_time"] is not None


@pytest.mark.usefixtures("skip_view_update")
def test_enabled_with_absolutely_no_beacons_reports_waiting(setup_temp_rum_db, _mock_rum_enabled_config):
    _ = setup_temp_rum_db
    service_id = "test_beacon_health_empty"
    # Database is completely empty

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 0
    assert data["setup_complete"] is False
    assert data["message"] == "Waiting for beacons..."
    assert data["last_beacon_time"] is None


@pytest.mark.usefixtures("skip_view_update")
def test_db_failure_is_caught_and_reported_in_message(setup_temp_rum_db, _mock_rum_enabled_config, monkeypatch):
    _ = setup_temp_rum_db
    service_id = "test_beacon_health_db_failure"

    def _raise(*args, **kwargs):
        raise RuntimeError("db is locked")

    monkeypatch.setattr("backend.routers.rum.execute_with_stale_view_retry", _raise)

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["setup_complete"] is False
    assert "Setup check failed" in data["message"]
    assert "db is locked" in data["message"]


GOLDEN_DIR = Path(__file__).parent / "golden"

_GOLDEN_SERVICE_ID = "svc-rum-golden"

WINDOW = "start_time=2026-08-05T00:00:00Z&end_time=2026-08-05T23:59:59Z"


@pytest.fixture
def rum_db(request):
    return request.getfixturevalue("setup_temp_rum_db")


def _beacons() -> list[dict]:
    out: list[dict] = []
    specs = [
        ("10:05:00", "/home", "Chrome", "macOS", "Desktop", 1.2, 2100.0, 0.05, 150.0, False),
        ("10:20:00", "/home", "Firefox", "Windows", "Desktop", 2.4, 3400.0, 0.15, 320.0, False),
        ("10:40:00", "/cart", "Chrome", "Android", "Mobile", 3.1, 4800.0, 0.3, 650.0, True),
        ("11:10:00", "/cart", "Safari", "macOS", "Desktop", 0.9, 1800.0, 0.02, 90.0, True),
        ("11:50:00", "/search", "Chrome", "Windows", "Desktop", 1.7, 2600.0, 0.11, 210.0, False),
        ("13:30:00", "/home", "Chrome", "macOS", "Desktop", 1.1, 2000.0, 0.04, 120.0, False),
    ]
    for i, (hms, path, browser, os_, device, load, lcp, cls, inp, err) in enumerate(specs):
        b = {
            "received_at": f"2026-08-05T{hms}+00:00",
            "pathname": path,
            "meta": {"browser": browser, "os": os_, "device": device},
            "load_time": load,
            "lcp": lcp,
            "cls": cls,
            "inp": inp,
            "cid": f"cid_{i}",
            "req_id": f"req_{i}",
        }
        if err:
            n = 2 if i == 2 else 1
            b["exceptions"] = [{"value": f"Boom {i}", "filename": "app.js"}] * n
        out.append(b)
    return out


def _check(name: str, body) -> None:
    if isinstance(body, dict):
        body = {k: v for k, v in body.items() if not k.startswith("_debug")}
    path = GOLDEN_DIR / f"rum_{name}.json"
    rendered = json.dumps(body, indent=2, sort_keys=True) + "\n"
    if os.getenv("UPDATE_GOLDEN") == "1":
        path.write_text(rendered)
    assert path.exists(), f"missing golden {path.name}; run with UPDATE_GOLDEN=1"
    assert json.loads(path.read_text()) == json.loads(rendered)


def _get_golden(url: str):
    r = client.get(url)
    assert r.status_code == 200, r.text
    return r.json()


@pytest.mark.usefixtures("skip_view_update")
def test_golden_empty(rum_db) -> None:
    _check("analytics_empty", _get_golden(f"/api/services/{_GOLDEN_SERVICE_ID}/rum/analytics?{WINDOW}"))


@pytest.mark.usefixtures("skip_view_update")
def test_golden_raw_long_window(rum_db) -> None:
    _insert_beacons(rum_db, _beacons())
    _check("analytics_raw_long", _get_golden(f"/api/services/{_GOLDEN_SERVICE_ID}/rum/analytics?{WINDOW}"))


@pytest.mark.usefixtures("skip_view_update")
def test_golden_raw_short_window(rum_db) -> None:
    _insert_beacons(rum_db, _beacons())
    url = f"/api/services/{_GOLDEN_SERVICE_ID}/rum/analytics?start_time=2026-08-05T10:00:00Z&end_time=2026-08-05T11:30:00Z"
    _check("analytics_raw_short", _get_golden(url))


@pytest.mark.usefixtures("skip_view_update")
def test_golden_filtered(rum_db) -> None:
    _insert_beacons(rum_db, _beacons())
    flt = json.dumps({"browser": {"mode": "include", "values": ["Chrome"]}})
    params = {"start_time": "2026-08-05T00:00:00Z", "end_time": "2026-08-05T23:59:59Z", "filters": flt}
    resp = client.get(f"/api/services/{_GOLDEN_SERVICE_ID}/rum/analytics", params=params)
    assert resp.status_code == 200, resp.text
    _check("analytics_filtered", resp.json())


@pytest.mark.usefixtures("skip_view_update")
def test_golden_rollup(rum_db) -> None:
    import backend.core.duckdb_pool as duckdb_pool
    from backend.core.rollups.rum import recompute_rum_aggregates

    _insert_beacons(rum_db, _beacons())
    duckdb_pool.shutdown_all()
    con = duckdb.connect(str(rum_db), read_only=False)
    try:
        recompute_rum_aggregates(con, _GOLDEN_SERVICE_ID)
        assert con.execute("SELECT COUNT(*) FROM rum_vitals_aggregates").fetchone()[0] > 0
    finally:
        con.close()
    _check("analytics_rollup", _get_golden(f"/api/services/{_GOLDEN_SERVICE_ID}/rum/analytics?{WINDOW}"))


@pytest.mark.usefixtures("skip_view_update")
def test_golden_live_events(rum_db) -> None:
    # One metric (or one error) per timestamp: live-events orders by timestamp only, so
    # same-timestamp rows would make the golden order nondeterministic.
    beacons = []
    for i, b in enumerate(_beacons()):
        base = {k: b[k] for k in ("pathname", "meta", "cid", "req_id")}
        beacons.append({**base, "received_at": b["received_at"], "lcp": b["lcp"]})
        if i % 2:
            beacons.append(
                {**base, "received_at": b["received_at"].replace(":00+", ":30+"), "exceptions": [{"value": f"Err {i}"}]}
            )
    _insert_beacons(rum_db, beacons)
    _check("live_events", {"events": _get_golden(f"/api/services/{_GOLDEN_SERVICE_ID}/rum/live-events?{WINDOW}")})


@pytest.mark.usefixtures("skip_view_update")
def test_golden_beacon_health_disabled(rum_db) -> None:
    _check("beacon_health_disabled", _get_golden(f"/api/services/{_GOLDEN_SERVICE_ID}/rum/beacon-health"))


_TOKEN_SERVICE_ID = "svc-rum-token"

ANCHOR = "2026-08-05T12:00:30Z"

EMPTY = {"no_data": True, "vitals_rows": []}


@pytest.fixture
def captured(setup_temp_rum_db):
    calls: list[dict] = []

    def fake(con, **kw):
        calls.append(kw)
        return EMPTY

    with patch("backend.routers.rum.rum_repo.fetch_rum_analytics", fake):
        yield calls


def _get_token(**params):
    r = client.get(f"/api/services/{_TOKEN_SERVICE_ID}/rum/analytics", params=params)
    assert r.status_code == 200, r.text
    return r.json()


@pytest.mark.usefixtures("skip_view_update")
def test_token_resolves_same_window_as_dashboard_helper(captured) -> None:
    _get_token(range_token="24h", anchor=ANCHOR)
    exp_start, exp_end = resolve_window("24h", ANCHOR)
    assert (captured[0]["start_time"], captured[0]["end_time"]) == (exp_start, exp_end)
    assert exp_end == "2026-08-05T12:00:00Z"


@pytest.mark.usefixtures("skip_view_update")
def test_token_ignores_supplied_absolute_bounds(captured) -> None:
    _get_token(range_token="1h", anchor=ANCHOR, start_time="2000-01-01T00:00:00Z", end_time="2000-01-02T00:00:00Z")
    assert (captured[0]["start_time"], captured[0]["end_time"]) == resolve_window("1h", ANCHOR)


@pytest.mark.usefixtures("skip_view_update")
def test_absent_token_uses_supplied_bounds_unchanged(captured) -> None:
    _get_token(start_time="2026-08-05T00:00:00Z", end_time="2026-08-05T23:59:59Z", anchor=ANCHOR)
    assert (captured[0]["start_time"], captured[0]["end_time"]) == ("2026-08-05T00:00:00Z", "2026-08-05T23:59:59Z")


@pytest.mark.usefixtures("skip_view_update")
def test_invalid_token_falls_back_to_supplied_bounds(captured) -> None:
    _get_token(range_token="bogus", anchor=ANCHOR, start_time="2026-08-05T00:00:00Z", end_time="2026-08-05T01:00:00Z")
    assert (captured[0]["start_time"], captured[0]["end_time"]) == ("2026-08-05T00:00:00Z", "2026-08-05T01:00:00Z")


def _ctx(source: dict, session: object | None, bounds: TimeBounds) -> RequestContext:
    from tests.conftest import make_test_request_context

    return make_test_request_context(
        service_id=_TOKEN_SERVICE_ID,
        source=source,
        session=session,
        read_only=True,
        time_bounds=bounds,
    )


@pytest.mark.usefixtures("skip_view_update")
@pytest.mark.security_regression
def test_analyst_token_window_is_clamped_to_invite(setup_temp_rum_db, captured) -> None:
    """A 30d token must never widen an analyst past their 1h invite window."""
    session = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None)
    req = SimpleNamespace(state=SimpleNamespace(analyst_session=session))
    bounds = get_analyst_time_bounds(typing.cast(typing.Any, req))
    source = {
        "name": "test_service",
        "service_id": "test_service",
        "duckdb_path": str(setup_temp_rum_db).replace(".rum.duckdb", ".duckdb"),
        "access_level": "read_write",
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }

    def run(ctx: RequestContext, request: object) -> None:
        asyncio.run(
            rum_router.rum_analytics(
                request=typing.cast(typing.Any, request),
                start_time=None,
                end_time=None,
                filters=None,
                ctx=ctx,
                range_token="30d",
                anchor=None,
            )
        )

    run(_ctx(source, session, bounds), req)
    run(_ctx(source, None, TimeBounds(None, None)), SimpleNamespace(state=SimpleNamespace(analyst_session=None)))

    def span_h(c: dict) -> float:
        s, e = parse_iso_utc(c["start_time"]), parse_iso_utc(c["end_time"])
        assert s is not None and e is not None
        return (e - s).total_seconds() / 3600

    assert span_h(captured[0]) <= 1.01, "analyst token window not clamped to invite"
    assert span_h(captured[1]) > 24 * 29, "admin negative control should get the full 30d window"


# ── Merged from tests/routers/test_rum_analytics_vitals.py ──
def _faro_beacon(path: str, lcp: float, cls: float, inp: float, load_time: float) -> dict:
    return {
        "meta": {
            "page": {"url": f"https://example.test{path}"},
            "browser": {"name": "Chrome", "version": "120.0", "mobile": False},
            "os": {"name": "macOS", "version": "14.0"},
        },
        "events": [
            {
                "name": "faro.performance.navigation",
                "attributes": {"pageLoadTime": str(load_time)},
            }
        ],
        "measurements": [
            {"type": "web-vitals", "values": {"lcp": lcp, "cls": cls, "inp": inp}},
        ],
    }


@pytest.mark.usefixtures("skip_view_update")
@time_machine.travel("2026-09-22 12:30:00+00:00", tick=False)
def test_real_faro_shaped_beacons_produce_correct_p75_and_avg_load_time(setup_temp_rum_db):
    service_id = "test_vitals_faro_shape"
    # 4 beacons on the same page: LCP values 1.0, 2.0, 3.0, 4.0 -> p75 index
    # int(4*0.75)=3 -> sorted[3] == 4.0. Load times 1,2,3,4 -> avg 2.5.
    beacons = [_faro_beacon("/home", lcp, lcp / 2, lcp * 50, lcp) for lcp in (1.0, 2.0, 3.0, 4.0)]
    _insert(setup_temp_rum_db, beacons)

    r = client.get(f"/api/services/{service_id}/rum/analytics")
    assert r.status_code == 200
    data = r.json()
    assert data["is_mock"] is False

    # Continuous interpolation: 75% between values
    assert data["vitals"]["lcp"]["p75"] == 3.25
    assert data["vitals"]["cls"]["p75"] == 1.625
    assert data["vitals"]["inp"]["p75"] == 162

    worst = data["worst_pages"]
    assert len(worst) == 1
    assert worst[0]["path"] == "/home"
    assert worst[0]["avg_load_time"] == 2.5
    assert worst[0]["lcp_p75"] == 3.25


@pytest.mark.usefixtures("skip_view_update")
def test_faro_exception_event_is_counted_and_grouped(setup_temp_rum_db):
    service_id = "test_vitals_faro_exception"
    beacon = _faro_beacon("/checkout", 2.0, 0.02, 100, 2.0)

    # Faro structured stacktrace frame to make sure extract works
    beacon["exceptions"] = [
        {
            "value": "TypeError: boom",
            "type": "TypeError",
            "stacktrace": {"frames": [{"filename": "app.js", "lineno": 42, "colno": 7}]},
        }
    ]
    # Need >=10 total beacons for the real-data path (not the "no_data" short-circuit).
    beacons = [beacon] + [_faro_beacon("/other", 1.0, 0.01, 50, 1.0) for _ in range(10)]
    _insert(setup_temp_rum_db, beacons)

    r = client.get(f"/api/services/{service_id}/rum/analytics")
    data = r.json()
    assert not data["is_mock"]
    errors = data["errors"]
    assert len(errors) == 1
    assert errors[0]["message"] == "TypeError: boom"
    assert errors[0]["file"] == "app.js"
    assert errors[0]["line"] == 42
    assert errors[0]["count"] == 1

    checkout_page = next(p for p in data["worst_pages"] if p["path"] == "/checkout")
    assert checkout_page["error_rate"] == 100.0


@pytest.mark.usefixtures("skip_view_update")
def test_exclude_filter_mode_removes_matching_beacons(setup_temp_rum_db):
    service_id = "test_vitals_exclude_filter"
    beacons = []
    for i in range(15):
        b = _faro_beacon(f"/p{i}", 1.5, 0.02, 80, 1.5)
        b["meta"]["browser"] = {"name": "Chrome"} if i < 5 else {"name": "Safari"}
        beacons.append(b)
    _insert(setup_temp_rum_db, beacons)

    filters = json.dumps({"browser": {"mode": "exclude", "values": ["Chrome"]}})
    r = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert r.status_code == 200
    data = r.json()
    assert not data["is_mock"]
    # 10 Safari beacons should remain; 5 Chrome beacons excluded.
    assert data["environments"]["browsers"] == {"Safari": 10}


@pytest.mark.usefixtures("skip_view_update")
def test_path_extraction_falls_back_to_page_url_when_no_pathname(setup_temp_rum_db):
    service_id = "test_vitals_url_fallback"
    beacons = []
    for i in range(12):
        b = {
            "meta": {
                "page": {"url": "https://example.test/from-url//nested"},
                "browser": {"name": "Chrome"},
                "os": {"name": "macOS"},
            },
            "events": [
                {
                    "name": "faro.performance.navigation",
                    "attributes": {"pageLoadTime": "1.5"},
                }
            ],
            "measurements": [
                {"type": "web-vitals", "values": {"lcp": 1.5, "cls": 0.02, "inp": 80}},
            ],
        }
        beacons.append(b)
    _insert(setup_temp_rum_db, beacons)

    r = client.get(f"/api/services/{service_id}/rum/analytics")
    data = r.json()
    assert not data["is_mock"]
    paths = [p["path"] for p in data["worst_pages"]]
    # DuckDB returns exact extracted pathname
    assert paths == ["/from-url//nested"]


@pytest.mark.usefixtures("skip_view_update")
def test_processing_exception_returns_500(setup_temp_rum_db):
    service_id = "test_vitals_mock_fallback"
    _insert(setup_temp_rum_db, [_faro_beacon("/x", 1.0, 0.01, 50, 1.0) for _ in range(11)])

    import backend.routers.rum as rum_module

    # Mock execute_with_stale_view_retry to raise RuntimeError
    def _exploding_execute(*args, **kwargs):
        raise RuntimeError("simulated DB corruption mid-query")

    original = rum_module.execute_with_stale_view_retry
    rum_module.execute_with_stale_view_retry = _exploding_execute
    try:
        r = client.get(f"/api/services/{service_id}/rum/analytics")
        assert r.status_code == 500
        data = r.json()
        assert data["detail"]["message"] == "RUM analytics query failed"
        assert "simulated DB corruption" not in r.text
        assert data["detail"]["error_id"]
    finally:
        rum_module.execute_with_stale_view_retry = original


# ── Merged from tests/routers/test_rum_enable_disable.py ──
_SVC_rum_enable_disable = "TestRumEnableDisableSvc"


def _events(response) -> list[dict]:
    return [json.loads(line[len("data: ") :]) for line in response.text.splitlines() if line.startswith("data: ")]


def test_enable_404_when_service_has_no_config(client, with_config):
    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/enable", json={})
    assert r.status_code == 404


def test_enable_400_when_no_token_anywhere(client, with_config):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": ""}
    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/enable", json={})
    assert r.status_code == 400
    assert "token" in r.json()["detail"]["error"].lower()


def test_enable_persists_a_freshly_supplied_token(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": ""}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/enable", json={"token": "NEW-TOKEN"})
    assert r.status_code == 200
    assert with_config[_SVC_rum_enable_disable]["fastly_api_key"] == "NEW-TOKEN"


def test_enable_happy_path_streams_done_with_activate_message(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": "TOKEN"}

    captured: dict = {}

    def fake_run_with_events(func, *args, **kwargs):
        captured["func"] = func
        captured["kwargs"] = kwargs
        yield {"type": "status", "message": "enabling RUM"}
        with_config[_SVC_rum_enable_disable] = {
            "service_id": _SVC_rum_enable_disable,
            "fastly_api_key": "TOKEN",
            "rum_enabled": True,
            "rum_enabled_at": "2026-08-09T00:00:00Z",
        }

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/enable", json={"activate": True})
    assert r.status_code == 200
    events = _events(r)
    types = [e["type"] for e in events]
    assert "done" in types
    done = next(e for e in events if e["type"] == "done")
    assert done["rum"]["enabled"] is True
    assert "enabled successfully" in done["message"]
    assert captured["kwargs"]["activate"] is True


def test_enable_draft_mode_uses_draft_message(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "compiling draft"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/enable", json={"activate": False})
    events = _events(r)
    done = next(e for e in events if e["type"] == "done")
    assert "draft configuration compiled" in done["message"]


def test_enable_streams_error_event_on_orchestrator_failure(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling"}
        raise RuntimeError("FOS bucket creation failed")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/enable", json={})
    assert r.status_code == 200
    events = _events(r)
    types = [e["type"] for e in events]
    assert "error" in types
    assert "done" not in types
    err = next(e for e in events if e["type"] == "error")
    assert "fos bucket" in err["message"].lower()


def test_enable_swallows_scheduler_reload_failure_and_still_reports_done(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling"}

    def _boom():
        raise RuntimeError("scheduler unavailable")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", _boom)

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/enable", json={})
    assert r.status_code == 200
    events = _events(r)
    # A broken scheduler reload must not prevent the 'done' event from
    # reaching the client — enabling RUM still succeeded.
    assert any(e["type"] == "done" for e in events)


def test_disable_400_when_no_token_anywhere(client, with_config):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": ""}
    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/disable", json={})
    assert r.status_code == 400


def test_disable_404_when_service_has_no_config_and_no_token_supplied(client, with_config):
    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/disable", json={})
    assert r.status_code == 404


def test_disable_happy_path_streams_done_and_reflects_disabled_state(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {
        "service_id": _SVC_rum_enable_disable,
        "fastly_api_key": "TOKEN",
        "rum_enabled": True,
    }

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "disabling RUM"}
        with_config[_SVC_rum_enable_disable] = {
            "service_id": _SVC_rum_enable_disable,
            "fastly_api_key": "TOKEN",
            "rum_enabled": False,
        }

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/disable", json={"activate": True})
    assert r.status_code == 200
    events = _events(r)
    done = next(e for e in events if e["type"] == "done")
    assert done["rum"]["enabled"] is False
    assert "disabled successfully" in done["message"]


def test_disable_draft_mode_uses_draft_message(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "compiling draft"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/disable", json={"activate": False})
    events = _events(r)
    done = next(e for e in events if e["type"] == "done")
    assert "draft configuration compiled" in done["message"]


def test_disable_uses_supplied_token_without_needing_config(client, with_config, monkeypatch):
    """token in the request body means _get_fastly_token() (which requires
    an existing config) is never consulted."""
    captured: dict = {}

    def fake_run_with_events(func, *args, **kwargs):
        captured["args"] = args
        yield {"type": "status", "message": "disabling"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/disable", json={"token": "SUPPLIED-TOKEN"})
    assert r.status_code == 200
    assert "SUPPLIED-TOKEN" in captured["args"]


def test_disable_streams_error_event_on_orchestrator_failure(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "disabling"}
        raise RuntimeError("bucket deletion failed")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/disable", json={})
    events = _events(r)
    err = next(e for e in events if e["type"] == "error")
    assert "bucket deletion" in err["message"].lower()


def test_disable_swallows_scheduler_reload_failure(client, with_config, monkeypatch):
    with_config[_SVC_rum_enable_disable] = {"service_id": _SVC_rum_enable_disable, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "disabling"}

    def _boom():
        raise RuntimeError("scheduler down")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", _boom)

    r = client.post(f"/api/services/{_SVC_rum_enable_disable}/rum/disable", json={})
    events = _events(r)
    assert any(e["type"] == "done" for e in events)


# ── Merged from tests/routers/test_rum_status_analyst_projection.py ──
_SVC_rum_status_analyst_projection = "svc-rum-status-projection"


def _request(analyst_session: object | None) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _analyst_session_rum_status_analyst_projection() -> SimpleNamespace:
    return SimpleNamespace(
        session_id="s-test", service_ids=[_SVC_rum_status_analyst_projection], email="analyst@example.com"
    )


@pytest.mark.security_regression
async def test_analyst_status_excludes_vcl_fingerprint_fields(with_config, monkeypatch):
    """No ``*_vcl_sha`` and no ``vcl_drift`` key must survive to an analyst,
    even when RUM is enabled and drifted."""
    with_config[_SVC_rum_status_analyst_projection] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "deadbeef",
    }
    # rum_vcl_fingerprint reads the deployed VCL — must not even be called
    # for an analyst caller (skip the fingerprint work entirely).
    monkeypatch.setattr(
        rum_router,
        "rum_vcl_fingerprint",
        lambda service_id: pytest.fail("rum_vcl_fingerprint must not be called for an analyst caller"),
    )

    result = await rum_router.rum_status(
        _request(_analyst_session_rum_status_analyst_projection()), service_id=_SVC_rum_status_analyst_projection
    )

    assert result == {"enabled": True, "enabled_at": "2026-01-01T00:00:00Z"}
    assert "deployed_vcl_sha" not in result
    assert "current_vcl_sha" not in result
    assert "vcl_drift" not in result


@pytest.mark.security_regression
async def test_analyst_can_read_enabled_field(with_config):
    """Analyst CAN read ``enabled`` — this is the field the RUM page gates
    its entire body on, and it is not operator-sensitive."""
    with_config[_SVC_rum_status_analyst_projection] = {"rum_enabled": False}

    result = await rum_router.rum_status(
        _request(_analyst_session_rum_status_analyst_projection()), service_id=_SVC_rum_status_analyst_projection
    )

    assert result["enabled"] is False
    assert "enabled_at" in result


def test_admin_status_still_sees_vcl_fingerprint_fields(with_config, monkeypatch):
    """Negative control: the admin (no analyst_session) branch is untouched —
    it still gets the full VCL-drift-detection payload."""
    with_config[_SVC_rum_status_analyst_projection] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "deadbeef",
    }
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "deadbeef")

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=_SVC_rum_status_analyst_projection))

    assert result["enabled"] is True
    assert result["deployed_vcl_sha"] == "deadbeef"
    assert result["current_vcl_sha"] == "deadbeef"
    assert result["vcl_drift"] is False


def test_admin_status_defaults_deployed_sha_to_current_when_enabled_but_unstored(with_config, monkeypatch):
    """Enabled with no ``rum_vcl_sha`` recorded yet (e.g. enabled by an older
    code path that never wrote it) must NOT report drift against itself —
    ``deployed_sha`` falls back to ``current_sha`` rather than staying None."""
    with_config[_SVC_rum_status_analyst_projection] = {"rum_enabled": True, "rum_enabled_at": "2026-01-01T00:00:00Z"}
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "freshsha123")

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=_SVC_rum_status_analyst_projection))

    assert result["deployed_vcl_sha"] == "freshsha123"
    assert result["current_vcl_sha"] == "freshsha123"
    assert result["vcl_drift"] is False


def test_admin_status_migrates_legacy_pre_f4_sha_instead_of_false_positive_drift(with_config, monkeypatch):
    """#2 audit finding: the F-4 fix made ``rum_vcl_fingerprint`` depend on
    ``faro_version``, but nothing migrated services whose ``rum_vcl_sha``
    was stored by the OLD algorithm (which never varied with
    ``faro_version`` at all). That stale value can never again match the
    new algorithm's output, so every pre-existing RUM service would report
    ``vcl_drift: true`` forever — even one that was just correctly
    reconciled.

    Pins the chosen fix (one-time migration on read): a stored sha that
    exactly matches what the OLD algorithm would have produced for this
    service is recognized as legacy-format (not real drift), cleared to
    ``vcl_drift: false`` for this response, AND persisted as the new
    algorithm's value so subsequent reads don't need the fallback again.
    """
    with_config[_SVC_rum_status_analyst_projection] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "legacy-sha-abc",
    }
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "current-sha-xyz")
    monkeypatch.setattr(rum_router, "legacy_rum_vcl_fingerprint", lambda service_id: "legacy-sha-abc")

    saved: dict = {}
    monkeypatch.setattr(
        "backend.config.mutate_config", mutate_via_save(lambda service_id, cfg: saved.update({service_id: cfg}))
    )

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=_SVC_rum_status_analyst_projection))

    assert result["vcl_drift"] is False
    assert result["deployed_vcl_sha"] == "current-sha-xyz"
    assert result["current_vcl_sha"] == "current-sha-xyz"
    # Migration persisted so the next read skips the legacy fallback.
    assert saved[_SVC_rum_status_analyst_projection]["rum_vcl_sha"] == "current-sha-xyz"


def test_admin_status_reports_genuine_drift_when_sha_matches_neither_algorithm(with_config, monkeypatch):
    """Negative control: a stored sha that matches NEITHER the legacy NOR
    the current algorithm's output is real drift (e.g. VCL changed
    upstream, or the service was reconciled against a different
    faro_version) and must still be reported — the legacy-migration
    fallback must not mask genuine drift."""
    with_config[_SVC_rum_status_analyst_projection] = {
        "rum_enabled": True,
        "rum_enabled_at": "2026-01-01T00:00:00Z",
        "rum_vcl_sha": "genuinely-stale-sha",
    }
    monkeypatch.setattr(rum_router, "rum_vcl_fingerprint", lambda service_id: "current-sha-xyz")
    monkeypatch.setattr(rum_router, "legacy_rum_vcl_fingerprint", lambda service_id: "legacy-sha-abc")

    save_calls: list = []
    monkeypatch.setattr(
        "backend.config.mutate_config", mutate_via_save(lambda service_id, cfg: save_calls.append((service_id, cfg)))
    )

    import asyncio

    result = asyncio.run(rum_router.rum_status(_request(None), service_id=_SVC_rum_status_analyst_projection))

    assert result["vcl_drift"] is True
    assert result["deployed_vcl_sha"] == "genuinely-stale-sha"
    assert result["current_vcl_sha"] == "current-sha-xyz"
    # No migration write for a sha that isn't recognized as legacy-format.
    assert save_calls == []


# ── Merged from tests/routers/test_rum_versions_upgrade.py ──
_SVC_rum_versions_upgrade = "TestRumVersionsUpgradeSvc"


@pytest.fixture
def _reset_faro_cache():
    from backend.core import faro_versions

    faro_versions.reset_versions_cache()
    yield
    faro_versions.reset_versions_cache()


@pytest.mark.usefixtures("_reset_faro_cache")
def test_versions_update_available(client, with_config, monkeypatch):
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade, "rum": {"faro_version": "2.8.2"}}

    async def fake_fetch():
        return ["2.9.0", "2.8.2", "2.8.1"]

    monkeypatch.setattr("backend.core.faro_versions.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{_SVC_rum_versions_upgrade}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] == ["2.9.0", "2.8.2", "2.8.1"]
    assert body["current"] == "2.8.2"
    assert body["latest"] == "2.9.0"
    assert body["update_available"] is True


@pytest.mark.usefixtures("_reset_faro_cache")
def test_versions_already_up_to_date(client, with_config, monkeypatch):
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade, "rum": {"faro_version": "2.9.0"}}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.core.faro_versions.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{_SVC_rum_versions_upgrade}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["current"] == "2.9.0"
    assert body["latest"] == "2.9.0"
    assert body["update_available"] is False


@pytest.mark.usefixtures("_reset_faro_cache")
def test_versions_unpinned_service(client, with_config, monkeypatch):
    """No ``rum.faro_version`` in config (never enabled / never upgraded) ->
    current is null and update_available is False even though a latest
    version is known — there's nothing to compare against yet."""
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.core.faro_versions.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{_SVC_rum_versions_upgrade}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["current"] is None
    assert body["latest"] == "2.9.0"
    assert body["update_available"] is False


@pytest.mark.usefixtures("_reset_faro_cache")
def test_versions_registry_failure_degrades_gracefully(client, with_config, monkeypatch):
    """A registry failure (down / rate-limited / malformed payload) with no
    cached list must degrade to a 200 with an empty ``available`` list — the
    version list is a non-critical "update available?" affordance, so a slow
    or unavailable npm registry must never 503 and break the whole RUM page
    (measured 2026-10-01: an uncached per-request fetch 503'd under
    verify-phase load and hard-failed the dashboard verifier)."""
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade, "rum": {"faro_version": "2.8.2"}}

    async def fake_fetch():
        raise ValueError("Failed to fetch Faro versions: registry returned 503")

    monkeypatch.setattr("backend.core.faro_versions.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{_SVC_rum_versions_upgrade}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] == []
    assert body["current"] == "2.8.2"
    assert body.get("latest") is None
    assert body["update_available"] is False


@pytest.mark.usefixtures("_reset_faro_cache")
def test_upgrade_rejects_unknown_version_before_orchestration(client, with_config, monkeypatch):
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade, "fastly_api_key": "TOKEN"}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    mock_run_with_events = MagicMock()
    monkeypatch.setattr("backend.routers.rum.run_with_events", mock_run_with_events)

    r = client.post(f"/api/services/{_SVC_rum_versions_upgrade}/rum/upgrade", json={"version": "9.9.9"})
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "unknown_faro_version"
    mock_run_with_events.assert_not_called()


@pytest.mark.usefixtures("_reset_faro_cache")
def test_upgrade_happy_path_invokes_orchestrator_and_streams_events(client, with_config, monkeypatch):
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade, "fastly_api_key": "TOKEN"}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    captured: dict = {}

    def fake_run_with_events(func, *args, **kwargs):
        captured["func"] = func
        captured["args"] = args
        captured["kwargs"] = kwargs
        yield {"type": "status", "message": "downloading v2.9.0"}
        # Simulate the orchestrator having pinned the new version on success.
        with_config[_SVC_rum_versions_upgrade] = {
            "service_id": _SVC_rum_versions_upgrade,
            "fastly_api_key": "TOKEN",
            "rum": {"faro_version": "2.9.0"},
        }

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{_SVC_rum_versions_upgrade}/rum/upgrade", json={"version": "2.9.0"})

    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")

    events = [json.loads(line[len("data: ") :]) for line in r.text.splitlines() if line.startswith("data: ")]
    types = [e["type"] for e in events]
    assert "status" in types
    assert "done" in types
    done = next(e for e in events if e["type"] == "done")
    assert done["rum"]["faro_version"] == "2.9.0"

    # Confirm the router called the real orchestration function (Task 6's
    # upgrade_faro_version) with the resolved token and requested version —
    # not some ad hoc reimplementation.
    assert captured["func"] is upgrade_faro_version
    assert captured["args"] == (_SVC_rum_versions_upgrade, "2.9.0", "TOKEN")
    assert captured["kwargs"]["activate"] is True


@pytest.mark.usefixtures("_reset_faro_cache")
def test_upgrade_streams_error_event_on_orchestrator_failure(client, with_config, monkeypatch):
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade, "fastly_api_key": "TOKEN"}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "downloading"}
        raise RuntimeError("upload failed: FOS 503")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{_SVC_rum_versions_upgrade}/rum/upgrade", json={"version": "2.9.0"})

    assert r.status_code == 200  # streaming endpoint always 200; error rides the body
    events = [json.loads(line[len("data: ") :]) for line in r.text.splitlines() if line.startswith("data: ")]
    types = [e["type"] for e in events]
    assert "error" in types
    assert "done" not in types
    err = next(e for e in events if e["type"] == "error")
    assert "fos" in err["message"].lower()


@pytest.mark.usefixtures("_reset_faro_cache")
def test_upgrade_400_when_no_token_anywhere(client, with_config, monkeypatch):
    with_config[_SVC_rum_versions_upgrade] = {"service_id": _SVC_rum_versions_upgrade, "fastly_api_key": ""}

    async def fake_fetch():
        return ["2.9.0"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    r = client.post(f"/api/services/{_SVC_rum_versions_upgrade}/rum/upgrade", json={"version": "2.9.0"})
    assert r.status_code == 400
    assert "api key" in r.json()["detail"]["error"].lower()


# ── Merged from tests/routers/test_web_vitals.py ──
def test_web_vitals_accepts_well_formed_payload():
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-1234",
            "name": "LCP",
            "value": 1850.5,
            "rating": "good",
            "pathname": "/dashboard",
            "navigation_type": "navigate",
            "delta": 1850.5,
        },
    )
    assert r.status_code == 200, r.text
    # Admin responses get _debug_calls / _debug_queries / _is_cached
    # injected by the middleware when DEBUG_RESPONSES is on; just assert
    # the contract field exists.
    assert r.json().get("ok") is True


def test_web_vitals_rejects_unknown_metric_name():
    """Pydantic Literal enforces the SDK's known-name set so a typo
    can't pollute the log stream with garbage metric names."""
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-bad",
            "name": "NOT_A_REAL_METRIC",
            "value": 0,
            "rating": "good",
        },
    )
    assert r.status_code == 422


def test_web_vitals_rejects_unknown_rating():
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-bad-rating",
            "name": "CLS",
            "value": 0.05,
            "rating": "amazing",  # invalid
        },
    )
    assert r.status_code == 422


def test_web_vitals_accepts_optional_fields_missing():
    """pathname / navigation_type / delta are optional — many SDK
    deliveries omit them (notably FCP / TTFB don't carry navigation_type
    in older browsers)."""
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-minimal",
            "name": "TTFB",
            "value": 220,
            "rating": "good",
        },
    )
    assert r.status_code == 200


def test_web_vitals_collection_off_by_default_persists_nothing(monkeypatch, tmp_path):
    """With WEB_VITALS_COLLECT unset the endpoint accepts the POST (200)
    but writes no sample — collection is opt-in."""
    monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    sink = tmp_path / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)

    r = client.post(
        "/api/web-vitals",
        json={"id": "off-1", "name": "LCP", "value": 1200, "rating": "good", "pathname": "/dashboard"},
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True
    assert not sink.exists()


def test_web_vitals_collection_on_persists_one_jsonl_line(monkeypatch, tmp_path):
    """With WEB_VITALS_COLLECT=1 the endpoint appends a flat, analyzer-
    friendly JSON line carrying the metric fields + a timestamp + cohort."""
    monkeypatch.setenv("WEB_VITALS_COLLECT", "1")
    sink = tmp_path / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)

    r = client.post(
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
    assert r.status_code == 200

    lines = sink.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["name"] == "LCP"
    assert rec["value"] == 2412.0
    assert rec["rating"] == "good"
    assert rec["pathname"] == "/dashboard"
    # Loopback test client is classified as admin; ts is stamped server-side.
    assert rec["cohort"] == "admin"
    assert rec["ts"].endswith("Z")


def test_page_telemetry_endpoint(monkeypatch, tmp_path):
    from backend import config as svcconfig
    from backend.core.metadata import usage_log, usage_log_db

    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "demo_service")

    usage_log.log_telemetry_query("demo_service", "page-456", "req-789", "duckdb", "SELECT 42", 12.3)

    response = client.get("/api/debug/page-telemetry?page_load_id=page-456")
    assert response.status_code == 200
    data = response.json()
    assert "queries" in data
    assert data["queries"][0]["sql_query"] == "SELECT 42"


def test_ux_events_accepts_well_formed_payload():
    r = client.post(
        "/api/ux-events",
        json={
            "event": "column_reordered",
            "pathname": "/sessions",
            "component_id": "Live Sessions",
            "details": {"column_id": "ja4", "from_index": 4, "to_index": 1, "column_count": 9},
        },
    )
    assert r.status_code == 200, r.text
    assert r.json().get("ok") is True


def test_ux_events_accepts_minimal_payload():
    """Only ``event`` is required — pathname / component_id / details
    are optional so the caller can opt out of context per-event."""
    r = client.post("/api/ux-events", json={"event": "filter_cleared"})
    assert r.status_code == 200, r.text


def test_ux_events_rejects_missing_event_field():
    r = client.post("/api/ux-events", json={"pathname": "/dashboard"})
    assert r.status_code == 422


def test_ux_events_rejects_extra_top_level_field():
    """``extra="forbid"`` so a typo in the SPA caller surfaces as a 422
    rather than silently landing in the log under an unread key."""
    r = client.post(
        "/api/ux-events",
        json={"event": "column_reordered", "typo_field": "ignored"},
    )
    assert r.status_code == 422


def test_ux_events_caps_event_name_length():
    """80-char cap keeps a misbehaving client from pumping arbitrary
    strings into the log stream."""
    r = client.post("/api/ux-events", json={"event": "x" * 81})
    assert r.status_code == 422


def test_ux_events_logs_admin_cohort_for_loopback_request(caplog):
    """Loopback request (no analyst session) is tagged ``admin`` so log
    slicing can split per-cohort."""
    import logging

    with caplog.at_level(logging.INFO, logger="backend.ux_events"):
        r = client.post("/api/ux-events", json={"event": "column_reordered"})
    assert r.status_code == 200
    matched = [rec for rec in caplog.records if rec.message == "ux_event"]
    assert matched, "expected at least one ux_event log record"
    assert getattr(matched[-1], "ux_cohort", None) == "admin"
    assert getattr(matched[-1], "ux_event", None) == "column_reordered"
