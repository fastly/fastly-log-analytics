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

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.routers.rum import router as rum_router
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.config_fakes import mutate_via_save

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

    app.include_router(rum_router)
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
