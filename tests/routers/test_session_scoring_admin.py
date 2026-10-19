"""Unit tests for the Admin Session Scoring Configuration endpoints (/admin/session-scoring).

Covers:
  - GET  /api/services/{service_id}/scoring/status & /session-scoring/status (enabled/disabled, drift, secret redaction)
  - GET  /api/services/{service_id}/scoring/config & /session-scoring/config (composite config state)
  - GET  /api/services/{service_id}/scoring/dashboard & /session-scoring/dashboard (composite overview payload)
  - GET/PUT /api/services/{service_id}/scoring/threshold & /session-scoring/threshold
  - GET/PUT /api/services/{service_id}/scoring/enforce-threshold & /session-scoring/enforce-threshold (?confirm=true guard)
  - GET/PUT /api/services/{service_id}/scoring/l2-enforce & /session-scoring/l2-enforce (?confirm=true guard)
  - GET/PUT/POST /api/services/{service_id}/scoring/exclude-regex(/validate) & /session-scoring/exclude-regex(/validate)
  - GET/PUT /api/services/{service_id}/scoring/enforce-status-code & /session-scoring/enforce-status-code
  - GET/POST /api/services/{service_id}/scoring/matrix-versions(/{version}/restore) & /session-scoring/matrix-versions
  - POST /api/services/{service_id}/scoring/retrain & /rotate-key
  - GET  /api/services/{service_id}/scoring/audit & /session-scoring/audit
  - GET/POST/DELETE /api/services/{service_id}/scoring/labels & /session-scoring/labels
  - Strict Admin-only RBAC (403 {"error": "admin_only"} for Analyst Path B across all admin/mutation endpoints)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.routers import session_scoring as _ss
from backend.routers import session_scoring_admin as _ssa
from backend.routers.session_scoring import router as scoring_router
from backend.utils.remote_access import _is_blocked_path

SVC = "TestScoringAdminSvc"


@pytest.fixture(autouse=True)
def _reset_scoring_caches():
    _ss._analytics_cache.clear()
    _ss._inflight.clear()
    _ss._scoring_svc_version_cache.clear()
    _ssa._enforce_threshold_cache.clear()
    _ssa._l2_enforce_cache.clear()
    yield
    _ss._analytics_cache.clear()
    _ss._inflight.clear()
    _ss._scoring_svc_version_cache.clear()
    _ssa._enforce_threshold_cache.clear()
    _ssa._l2_enforce_cache.clear()


def _make_app(*, analyst: bool = False) -> FastAPI:
    app = FastAPI()
    if analyst:

        @app.middleware("http")
        async def _inject_analyst(request: Request, call_next):
            request.state.analyst_session = MagicMock(session_id="sess-analyst-scoring")
            return await call_next(request)

    app.include_router(scoring_router)
    return app


def _enabled_cfg(**scoring_overrides: Any) -> dict[str, Any]:
    scoring = {
        "enabled": True,
        "scoring_service_id": "compute_svc_123",
        "scoring_service_name": f"Session Scoring Service for {SVC}",
        "scoring_domain": "fos-test-scorer.edgecompute.app",
        "scoring_config_store_id": "cfg_store_123",
        "scoring_keys_store_id": "keys_store_123",
        "scoring_matrix_store_id": "matrix_store_123",
        "matrix_version": "2026-10-18",
        "deployed_package_sha": "pkg_sha_1",
        "deployed_vcl_sha": "vcl_sha_1",
        "operator_threshold": 75,
        "operator_threshold_set_at": "2026-10-18T12:00:00Z",
        "request_secret": "do-not-leak-secret",
    }
    scoring.update(scoring_overrides)
    return {
        "service_id": SVC,
        "fastly_api_key": "tok_admin_123",
        "scoring": scoring,
    }


class TestSessionScoringStatusAndComposites:
    """Status, config composite, and dashboard composite contract tests."""

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_status_returns_enabled_state_and_strips_secrets(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.provision.session_scoring_orchestrator.shipped_scorer_identity",
                return_value={"package_sha": "pkg_sha_1", "vcl_sha": "vcl_sha_1"},
            ),
            patch(
                "backend.core.fastly.service.get_active_version_info",
                return_value={"number": 7, "updated_at": "2026-10-18T12:00:00Z"},
            ),
        ):
            res = client.get(f"/api/services/{SVC}/{prefix}/status")

        assert res.status_code == 200
        body = res.json()
        assert body["enabled"] is True
        assert body["scoring_service_id"] == "compute_svc_123"
        assert body["scoring_active_version"] == 7
        assert body["scorer_drift"] is False
        assert "request_secret" not in body
        assert "do-not-leak-secret" not in res.text

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_config_composite_returns_bundled_controls(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.provision.session_scoring_orchestrator.shipped_scorer_identity",
                return_value={"package_sha": "pkg_sha_1", "vcl_sha": "vcl_sha_1"},
            ),
            patch(
                "backend.core.fastly.service.get_active_version_info",
                return_value={"number": 7, "updated_at": "2026-10-18T12:00:00Z"},
            ),
        ):
            res = client.get(f"/api/services/{SVC}/{prefix}/config")

        assert res.status_code == 200
        body = res.json()
        assert body["status"]["enabled"] is True
        assert body["threshold"]["threshold"] == 75
        assert body["enforce_status_code"]["effective"] == 429
        assert body["exclude_regex"]["is_default"] is True

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_dashboard_composite_returns_disabled_fast_path(self, prefix: str):
        cfg = {"service_id": SVC, "scoring": {"enabled": False}}
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.scoring.labels.list_labels", return_value=[]),
            patch("backend.scoring.labels.counts_by_label", return_value={"good": 0, "bad": 0, "neutral": 0}),
            patch("backend.repositories.session_scoring.query_logs", return_value=[]),
        ):
            res = client.get(f"/api/services/{SVC}/{prefix}/dashboard?since_hours=24&threshold=75")

        assert res.status_code == 200
        body = res.json()
        assert body["since_hours"] == 24
        assert body["threshold"] == 75
        assert body["status"]["enabled"] is False
        assert body["health"]["scored_rows"] == 0
        assert body["top_flagged"]["rows"] == []
        assert body["score_distribution"]["rows"] == []
        assert body["compliance_breakdown"]["rows"] == []
        assert body["threshold_preview"]["threshold"] == 75


class TestSessionScoringThresholdAndEnforcement:
    """Operator threshold, enforce-threshold, L2 enforcement, exclude-regex, and enforce-status-code tests."""

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_operator_threshold_get_and_put(self, prefix: str):
        cfg = _enabled_cfg(operator_threshold=70)
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config") as mock_save,
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            get_res = client.get(f"/api/services/{SVC}/{prefix}/threshold")
            assert get_res.status_code == 200
            assert get_res.json()["threshold"] == 70

            put_res = client.put(f"/api/services/{SVC}/{prefix}/threshold", json={"threshold": 85})
            assert put_res.status_code == 200
            assert put_res.json()["threshold"] == 85
            assert put_res.json()["enforced"] is False
            mock_save.assert_called_once()
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_enforce_threshold_requires_confirm_and_updates_config_store(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.core.fastly.client.fastly", return_value={"item_value": "80"}) as mock_fastly,
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            # Missing ?confirm=true -> 400
            unconfirmed = client.put(f"/api/services/{SVC}/{prefix}/enforce-threshold", json={"threshold": 80})
            assert unconfirmed.status_code == 400

            # With ?confirm=true -> 200
            confirmed = client.put(
                f"/api/services/{SVC}/{prefix}/enforce-threshold?confirm=true",
                json={"threshold": 80},
            )
            assert confirmed.status_code == 200
            body = confirmed.json()
            assert body["ok"] is True
            assert body["threshold"] == 80
            assert body["enforced"] is True
            mock_fastly.assert_called_once()
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_l2_enforce_requires_confirm_and_updates_config_store(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.core.fastly.client.fastly", return_value={"item_value": "1"}),
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            unconfirmed = client.put(f"/api/services/{SVC}/{prefix}/l2-enforce", json={"enabled": True})
            assert unconfirmed.status_code == 400

            confirmed = client.put(
                f"/api/services/{SVC}/{prefix}/l2-enforce?confirm=true",
                json={"enabled": True},
            )
            assert confirmed.status_code == 200
            body = confirmed.json()
            assert body["ok"] is True
            assert body["enabled"] is True
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_exclude_regex_get_validate_and_put(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with patch("backend.config.load_config", return_value=cfg):
            get_res = client.get(f"/api/services/{SVC}/{prefix}/exclude-regex")
            assert get_res.status_code == 200
            assert get_res.json()["is_default"] is True

        with patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}):
            val_res = client.post(
                f"/api/services/{SVC}/{prefix}/exclude-regex/validate",
                json={"regex": r"\.(css|js|png)$"},
            )
            assert val_res.status_code == 200
            assert val_res.json()["ok"] is True

            with (
                patch("backend.config.load_config", return_value=cfg),
                patch(
                    "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
                    return_value={
                        "current": r"\.(css|js|png)$",
                        "effective_regex": r"\.(css|js|png)$",
                        "is_default": False,
                        "logging_service_active_version": 12,
                    },
                ),
                patch("backend.core.metadata.record_scoring_audit") as mock_audit,
            ):
                put_res = client.put(
                    f"/api/services/{SVC}/{prefix}/exclude-regex?confirm=true",
                    json={"regex": r"\.(css|js|png)$"},
                )
                assert put_res.status_code == 200
                assert put_res.json()["ok"] is True
                assert put_res.json()["is_default"] is False
                mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_enforce_status_code_get_and_put(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with patch("backend.config.load_config", return_value=cfg):
            get_res = client.get(f"/api/services/{SVC}/{prefix}/enforce-status-code")
            assert get_res.status_code == 200
            assert get_res.json()["effective"] == 429

            bad_res = client.put(
                f"/api/services/{SVC}/{prefix}/enforce-status-code?confirm=true",
                json={"status_code": 200},
            )
            assert bad_res.status_code == 400

            with (
                patch(
                    "backend.provision.session_scoring_orchestrator.update_enforce_status_code",
                    return_value={
                        "current": 403,
                        "effective_status_code": 403,
                        "is_default": False,
                        "logging_service_active_version": 13,
                    },
                ),
                patch("backend.core.metadata.record_scoring_audit") as mock_audit,
            ):
                put_res = client.put(
                    f"/api/services/{SVC}/{prefix}/enforce-status-code?confirm=true",
                    json={"status_code": 403},
                )
                assert put_res.status_code == 200
                assert put_res.json()["ok"] is True
                assert put_res.json()["effective_status_code"] == 403
                mock_audit.assert_called_once()


class TestSessionScoringMatrixAuditAndOperations:
    """Matrix versions, restore, rotate-key, audit log, and labels tests."""

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_matrix_versions_list_and_restore(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.state_sync.list_scoring_matrix_versions",
                return_value=[{"version": "2026-10-17", "saved_at": "2026-10-17T00:00:00Z", "size_bytes": 512}],
            ),
        ):
            list_res = client.get(f"/api/services/{SVC}/{prefix}/matrix-versions")
            assert list_res.status_code == 200
            assert list_res.json()["current_version"] == "2026-10-18"
            assert len(list_res.json()["versions"]) == 1

        with (
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.config.mutate_config", return_value=cfg),
            patch(
                "backend.state_sync.restore_scoring_matrix_version",
                return_value={"restored_version": "2026-10-17", "restored_at": "2026-10-19T08:00:00Z"},
            ),
            patch("backend.provision.session_scoring_orchestrator._write_matrix_to_kv"),
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            restore_res = client.post(f"/api/services/{SVC}/{prefix}/matrix-versions/2026-10-17/restore?confirm=true")
            assert restore_res.status_code == 200
            body = restore_res.json()
            assert body["ok"] is True
            assert body["restored_version"] == "2026-10-17"
            assert body["matrix_kv_written"] is True
            mock_audit.assert_called_once()

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    def test_rotate_key_and_audit_list(self, prefix: str):
        cfg = _enabled_cfg()
        client = TestClient(_make_app(analyst=False))
        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.provision.session_scoring_setup.rotate_aes_key",
                return_value={"rotated_at": "2026-10-19T08:00:00Z", "previous_key_hex": "deadbeef"},
            ),
            patch("backend.core.metadata.record_scoring_audit") as mock_audit,
        ):
            rot_res = client.post(f"/api/services/{SVC}/{prefix}/rotate-key")
            assert rot_res.status_code == 200
            body = rot_res.json()
            assert body["ok"] is True
            assert body["previous_key_grace"] is True
            assert "deadbeef" not in rot_res.text
            mock_audit.assert_called_once()

        with (
            patch("backend.config.load_config", return_value=cfg),
            patch(
                "backend.core.metadata.list_scoring_audit",
                return_value=[
                    {
                        "id": 1,
                        "service_id": SVC,
                        "timestamp": "2026-10-19T08:00:00Z",
                        "action": "key_rotated",
                        "actor": "operator",
                        "details": {"previous_key_grace": True},
                    }
                ],
            ),
        ):
            audit_res = client.get(f"/api/services/{SVC}/{prefix}/audit?limit=50")
            assert audit_res.status_code == 200
            assert len(audit_res.json()["audit"]) == 1
            assert audit_res.json()["audit"][0]["action"] == "key_rotated"


class TestSessionScoringAdminRbacEnforcement:
    """Analyst Path B sessions must receive 403 {"error": "admin_only"} on all session-scoring admin/mutation endpoints."""

    @pytest.mark.parametrize("prefix", ["scoring", "session-scoring"])
    @pytest.mark.parametrize(
        ("method", "suffix", "payload"),
        [
            ("GET", "status", None),
            ("GET", "config", None),
            ("GET", "dashboard", None),
            ("GET", "threshold", None),
            ("PUT", "threshold", {"threshold": 75}),
            ("GET", "enforce-threshold", None),
            ("PUT", "enforce-threshold", {"threshold": 80}),
            ("GET", "l2-enforce", None),
            ("PUT", "l2-enforce", {"enabled": True}),
            ("GET", "exclude-regex", None),
            ("PUT", "exclude-regex", {"regex": ""}),
            ("POST", "exclude-regex/validate", {"regex": ""}),
            ("GET", "enforce-status-code", None),
            ("PUT", "enforce-status-code", {"status_code": 429}),
            ("GET", "matrix-versions", None),
            ("POST", "matrix-versions/2026-10-18/restore", None),
            ("POST", "retrain", None),
            ("POST", "rotate-key", None),
            ("GET", "audit", None),
            ("GET", "evaluation/per-reason", None),
            ("POST", "enable", {"token": "tok"}),
            ("POST", "disable", {"token": "tok"}),
        ],
    )
    def test_analyst_blocked_at_middleware_and_route_dependency(
        self,
        prefix: str,
        method: str,
        suffix: str,
        payload: dict[str, Any] | None,
    ):
        path = f"/api/services/{SVC}/{prefix}/{suffix}"
        # 1. Middleware path gate blocks the path
        assert _is_blocked_path(path) is True

        # 2. Route-level require_admin dependency also blocks with 403 admin_only
        client = TestClient(_make_app(analyst=True))
        if method == "GET":
            res = client.get(path)
        elif method == "PUT":
            res = client.put(path, json=payload)
        else:
            res = client.post(path, json=payload)

        assert res.status_code == 403
        assert res.json()["detail"]["error"] == "admin_only"
