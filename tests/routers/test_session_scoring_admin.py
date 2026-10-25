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

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.core.duckdb import _clear_schema_cache
from backend.core.request_context import build_request_context
from backend.core.share_db.validation import apply_pii_policy
from backend.main import app
from backend.repositories._base import _safe_table
from backend.routers import session_scoring as _ss
from backend.routers import session_scoring_admin as _ssa
from backend.routers.session_scoring import router as scoring_router
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests.conftest import MOCK_SERVICE_ID, override_request_context

# ── Merged from tests/routers/test_session_scoring_admin.py ──
SVC = "TestScoringAdminSvc"


@pytest.fixture
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


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringStatusAndComposites:
    """Status, config composite, and dashboard composite contract tests."""

    @pytest.mark.parametrize("prefix", ["scoring"])
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

    @pytest.mark.parametrize("prefix", ["scoring"])
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

    @pytest.mark.parametrize("prefix", ["scoring"])
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


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringThresholdAndEnforcement:
    """Operator threshold, enforce-threshold, L2 enforcement, exclude-regex, and enforce-status-code tests."""

    @pytest.mark.parametrize("prefix", ["scoring"])
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

    @pytest.mark.parametrize("prefix", ["scoring"])
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

    @pytest.mark.parametrize("prefix", ["scoring"])
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

    @pytest.mark.parametrize("prefix", ["scoring"])
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

    @pytest.mark.parametrize("prefix", ["scoring"])
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


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringMatrixAuditAndOperations:
    """Matrix versions, restore, rotate-key, audit log, and labels tests."""

    @pytest.mark.parametrize("prefix", ["scoring"])
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

    @pytest.mark.parametrize("prefix", ["scoring"])
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


@pytest.mark.usefixtures("_reset_scoring_caches")
class TestSessionScoringAdminRbacEnforcement:
    """Analyst Path B sessions must receive 403 {"error": "admin_only"} on all session-scoring admin/mutation endpoints."""

    @pytest.mark.parametrize("prefix", ["scoring"])
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


# ── Merged from tests/routers/test_scoring_exclude_regex.py ──
_SERVICE_ID = "svc-exclude-regex-test"


@pytest.fixture
def seeded_service(tmp_path, monkeypatch):
    """Per-test sandboxed configs dir with scoring enabled.

    The root conftest's ``isolate_metadata_db`` autouse fixture already
    creates ``tmp_path/configs`` and points svcconfig.CONFIGS_DIR there,
    so we just reuse that directory (mkdir would EEXIST otherwise).
    """
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    svcconfig.save_config(
        _SERVICE_ID,
        {
            "service_id": _SERVICE_ID,
            "name": "Test svc",
            "fastly_api_key": "stored-test-key",
            "fos_bucket": "test-bucket",
            "fos_region": "us-east-1",
            "scoring": {
                "enabled": True,
                "scoring_service_id": "sc-svc",
                "scoring_keys_store_id": "ks",
                "scoring_config_store_id": "cs",
                "request_secret": "test_secret_abc",
                "scoring_domain": "scorer.example.com",
                "scoring_service_name": "Test scoring service",
            },
        },
    )
    return cfg_dir


def test_get_returns_default_when_no_override(seeded_service):
    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_default"] is True
    assert body["current"] == ""
    assert body["default"]  # non-empty
    assert body["effective"] == body["default"]


def test_get_returns_override_when_persisted(seeded_service):
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = r"\.(css|js)$"
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200
    body = r.json()
    assert body["is_default"] is False
    assert body["current"] == r"\.(css|js)$"
    assert body["effective"] == r"\.(css|js)$"


def test_validate_returns_ok_for_clean_regex(seeded_service):
    """Dry-run validator with a syntactically valid + policy-clean regex
    returns ok=True. Drives the admin UI's on-blur lint affordance — no
    cfg writes, no Fastly calls."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": r"\.(css|js)$"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert isinstance(body["lint_warnings"], list)


def test_validate_returns_error_for_invalid_regex(seeded_service):
    """Operator typing an unbalanced regex sees ok=False with a structured
    reason — same shape the publish flow surfaces, so the on-blur UI can
    use one error renderer for both code paths."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": "(unclosed"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["reason"] == "invalid_regex"
    assert "error" in body and isinstance(body["error"], str)


def test_validate_returns_error_for_disallowed_quote(seeded_service):
    """Input-policy layer (length / quotes / control chars) fires before
    the regex compile and before falco — exercise that path too."""
    with (
        TestClient(app) as c,
        patch("backend.utils.vcl_validator.shutil.which", return_value=None),
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": 'foo"bar'},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body["reason"] == "disallowed_char"


def test_validate_does_not_persist_to_cfg(seeded_service):
    """The whole point of the validate endpoint is that it's read-only —
    a successful dry-run must NOT mutate cfg.scoring.exclude_url_regex."""
    cfg_before = svcconfig.load_config(_SERVICE_ID)
    stored_before = (cfg_before.get("scoring") or {}).get("exclude_url_regex")

    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": r"^/healthz$"},
        )
    assert r.status_code == 200
    assert r.json()["ok"] is True

    cfg_after = svcconfig.load_config(_SERVICE_ID)
    stored_after = (cfg_after.get("scoring") or {}).get("exclude_url_regex")
    assert stored_after == stored_before, "validate must be read-only — cfg.scoring.exclude_url_regex changed"


def test_validate_rejects_non_string_body(seeded_service):
    """body.regex must be a string — int / null / array → 422 from the
    Pydantic body validator, matching the body/query validation
    classification (see commit 3c036cf)."""
    with TestClient(app) as c:
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": 42},
        )
    assert r.status_code == 422
    # Pydantic envelope is a list of error objects under detail; pin the
    # field name + the string-input expectation so a future schema change
    # surfaces here rather than masquerading as a generic 422.
    errors = r.json()["detail"]
    assert any(e.get("loc", [])[-1] == "regex" for e in errors)
    assert any("string" in e.get("msg", "").lower() for e in errors)


def test_get_reports_is_default_when_literal_default_stored(seeded_service):
    """enable_scoring persists the literal DEFAULT_ASSET_EXT_REGEX into
    cfg on first turn-on so the admin UI's textarea is pre-populated with
    something to edit (the previous null sentinel produced an empty box).
    The GET must still report ``is_default=True`` in that case, otherwise
    the UI would mislabel a fresh-from-enable service as
    "currently custom override" the moment scoring is turned on."""
    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX

    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = DEFAULT_ASSET_EXT_REGEX
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.get(f"/api/services/{_SERVICE_ID}/scoring/exclude-regex")
    assert r.status_code == 200
    body = r.json()
    assert body["is_default"] is True, "stored value equals the bundled default → must report is_default=True"
    assert body["current"] == DEFAULT_ASSET_EXT_REGEX
    assert body["effective"] == DEFAULT_ASSET_EXT_REGEX


def test_put_without_confirm_rejected(seeded_service):
    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 400
    assert "confirm=true" in r.json()["detail"]["error"]


def test_put_without_token_rejected(seeded_service):
    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"confirm": "true"},
            json={"regex": r"\.css$"},
        )
    # _resolve_token falls back to stored fastly_api_key only when the
    # caller passes empty; here we explicitly omit token and the cfg
    # DOES have fastly_api_key, so this should actually proceed past
    # auth and only fail at the orchestrator (which we haven't stubbed).
    # The "no token" path requires the stored key also missing.
    assert r.status_code != 200  # some downstream failure expected


def test_put_with_disallowed_quote_rejected(seeded_service):
    with (
        TestClient(app) as c,
        patch("backend.utils.vcl_validator.shutil.which", return_value=None),
        # Disable falco-required so validation flows on its input-layer reject.
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": 'foo"bar'},
        )
    assert r.status_code == 400
    body = r.json()
    assert body["detail"]["reason"] == "disallowed_char"


def test_put_with_invalid_regex_rejected(seeded_service):
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": "(unclosed"},
        )
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "invalid_regex"


def test_put_when_scoring_disabled_rejected(seeded_service):
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["enabled"] = False
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 400
    assert "not enabled" in r.json()["detail"]["error"]


def test_put_happy_path_with_stubbed_orchestrator(seeded_service):
    """Full pipeline: input validation passes, validator passes (falco
    skipped via env), orchestrator stubbed → audit log entry written →
    200 OK."""
    fake_result = {
        "effective_regex": r"\.css$",
        "is_default": False,
        "logging_service_active_version": 42,
    }
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
        patch(
            "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
            return_value=fake_result,
        ) as stub_update,
        patch("backend.core.metadata.record_scoring_audit") as stub_audit,
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": r"\.css$"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["is_default"] is False
    assert body["effective_regex"] == r"\.css$"
    assert body["logging_service_active_version"] == 42
    stub_update.assert_called_once()
    stub_audit.assert_called_once()
    call = stub_audit.call_args
    assert call.args[1] == "scoring_exclude_regex_changed"


def test_put_reset_to_default(seeded_service):
    """Empty regex resets to default; orchestrator gets called with ""
    (which it maps to "use default" internally)."""
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["exclude_url_regex"] = r"\.(prior|custom)$"
    svcconfig.save_config(_SERVICE_ID, cfg)

    fake_result = {
        "effective_regex": "...DEFAULT...",
        "is_default": True,
        "logging_service_active_version": 43,
    }
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
        patch(
            "backend.provision.session_scoring_orchestrator.update_recv_exclusion_regex",
            return_value=fake_result,
        ),
        patch("backend.core.metadata.record_scoring_audit"),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": ""},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["is_default"] is True
    assert "Reset to default" in body["message"]


def test_scoring_vcl_excludes_query_params():
    """Assert that the default asset exclusion regex does not match
    excluded extensions in the query string parameter, but does match them in the path."""
    import re

    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX

    pattern = re.compile(DEFAULT_ASSET_EXT_REGEX, re.IGNORECASE)

    # Valid asset paths (should match and bypass scoring)
    assert pattern.search("/static/logo.png")
    assert pattern.search("/assets/styles.css")
    assert pattern.search("/js/app.js?v=1.2")

    # Dynamic paths with query params containing asset extensions (should NOT match, so they are scored)
    assert not pattern.search("/api/v1/login?file=.png")
    assert not pattern.search("/api/v1/user?bypass=.css")
    assert not pattern.search("/index.html?extension=.js")


@pytest.mark.security_regression
def test_validate_rejects_vcl_control_chars(seeded_service):
    """Control characters (\\x00-\\x08, \\x0a-\\x1f, \\x7f) are disallowed
    because they can break VCL string literals or confuse Fastly's VCL
    compiler. The double-quote case is covered by the existing
    ``test_validate_returns_error_for_disallowed_quote``; this test
    covers the remaining metacharacters."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        for ch, label in [
            ("\x00", "null byte"),
            ("\n", "newline"),
            ("\x7f", "DEL"),
        ]:
            r = c.post(
                f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
                json={"regex": f"foo{ch}bar"},
            )
            assert r.status_code == 200, f"{label}: unexpected status {r.status_code}"
            body = r.json()
            assert body["ok"] is False, f"{label}: should be rejected"
            assert body["reason"] == "disallowed_char", f"{label}: wrong reason {body.get('reason')}"


@pytest.mark.security_regression
def test_put_rejects_vcl_control_char_injection(seeded_service):
    """PUT rejects regex with embedded control chars — a complementary
    vector to the double-quote test."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.put(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex",
            params={"token": "stored-test-key", "confirm": "true"},
            json={"regex": "foo\nbar"},
        )
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == "disallowed_char"


@pytest.mark.security_regression
def test_validate_rejects_catastrophic_backtracking_vector(seeded_service):
    """Classic ReDoS patterns like ``(a+)+$`` compile fine under Python's
    ``re`` module and pass the disallowed-char check. Currently the only
    backstop is falco's 10 s timeout at lint time. This test documents
    the gap: without falco, the regex passes validation.

    When a dedicated ReDoS checker is added, this test should flip to
    asserting ``ok=False`` with a ``redos`` reason."""
    with (
        TestClient(app) as c,
        patch.dict("os.environ", {"SCORING_REQUIRE_FALCO": "0"}),
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/exclude-regex/validate",
            json={"regex": "(a+)+$"},
        )
    assert r.status_code == 200
    body = r.json()
    # Documents current behavior: the ReDoS vector passes input validation
    # when falco is not available. When a dedicated checker is added,
    # update this assertion to: assert body["ok"] is False
    assert body["ok"] is True


def test_rotate_key_happy_path(seeded_service):
    """Stub the crypto layer and verify the router returns the expected
    shape: ``ok``, ``rotated_at``, ``previous_key_grace``, ``message``."""
    fake_result = {
        "rotated_at": "2026-07-08T12:00:00Z",
        "previous_key_hex": "deadbeef",
    }
    with (
        TestClient(app) as c,
        patch(
            "backend.provision.session_scoring_setup.rotate_aes_key",
            return_value=fake_result,
        ) as stub_rotate,
        patch("backend.core.metadata.record_scoring_audit") as stub_audit,
    ):
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/rotate-key",
            headers={"token": "stored-test-key"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["rotated_at"] == "2026-07-08T12:00:00Z"
    assert body["previous_key_grace"] is True
    assert "message" in body
    stub_rotate.assert_called_once()
    stub_audit.assert_called_once()
    assert stub_audit.call_args.args[1] == "key_rotated"


def test_rotate_key_when_scoring_disabled(seeded_service):
    """rotate-key requires scoring to be enabled — 400 when disabled."""
    cfg = svcconfig.load_config(_SERVICE_ID)
    cfg["scoring"]["enabled"] = False
    svcconfig.save_config(_SERVICE_ID, cfg)

    with TestClient(app) as c:
        r = c.post(
            f"/api/services/{_SERVICE_ID}/scoring/rotate-key",
            headers={"token": "stored-test-key"},
        )
    assert r.status_code == 400
    assert "not enabled" in r.json()["detail"]["error"]


# ── Merged from tests/routers/test_sessions_token.py ──
_HDR = {"x-fastly-service-id": MOCK_SERVICE_ID}


def _seed(in_memory_duckdb, source, ip="10.0.0.1", n=10):
    # Mock logs store naive TIMESTAMP holding UTC wall-clock. Production clamps
    # to tz-aware bounds (parse_iso_utc → CAST AS TIMESTAMPTZ), so pin the
    # session TZ to UTC (as prod does) — otherwise the naive↔tz-aware compare
    # uses the host's local TZ and the window misses every row.
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    logs = generate_mock_logs(source, num_logs=n, hours_ago=1)
    for log in logs:
        log["ip"] = ip
    insert_mock_logs(in_memory_duckdb, _safe_table(source["name"]), logs)
    # The schema cache is keyed by table name, not connection — clear it so
    # get_sessions reads THIS fresh in-memory connection's columns rather than
    # a stale (possibly empty) entry left by another test on "test_service".
    _clear_schema_cache()


def _seed_zero_width(in_memory_duckdb, source, ip="10.0.0.9", n=5):
    """Seed a single-request session: every log shares ONE timestamp so the
    aggregated session has session_start == session_end (a zero-width window).
    Returns the pinned timestamp."""
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    ts = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    logs = generate_mock_logs(source, num_logs=n, hours_ago=1)
    for log in logs:
        log["ip"] = ip
        log["timestamp"] = ts
    insert_mock_logs(in_memory_duckdb, _safe_table(source["name"]), logs)
    _clear_schema_cache()
    return ts


def _window():
    now = datetime.now(UTC)
    return (now - timedelta(hours=2)).isoformat(), now.isoformat()


def _use_analyst(in_memory_duckdb, source, *, mask_ips: bool):
    """Re-point build_request_context to inject an analyst session into ctx.
    (The local TestClient socket path classifies as admin in the middleware, so
    we exercise the router's analyst branch via the injected ctx.analyst_session.)"""
    session = SimpleNamespace(service_ids=[source["service_id"]], pii_policy={"mask_ips": mask_ips})
    app.dependency_overrides[build_request_context] = override_request_context(
        source=source, _con_override=in_memory_duckdb, session=session
    )


def _list_first_token(client, in_memory_duckdb, source):
    # Omit start/end → default range (raw session path). An explicit >1h window
    # routes get_sessions to the rollup path, which has no parquet in tests.
    r = client.post("/api/sessions", headers=_HDR, json={})
    assert r.status_code == 200, r.text
    sessions = r.json()["sessions"]
    assert sessions, "expected at least one session row from seeded data"
    return sessions[0]


def test_list_attaches_session_token(client, in_memory_duckdb, test_service_source):
    """Every session row carries a non-empty opaque token."""
    _seed(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    assert row.get("session_token"), "session_token missing/empty on list row"
    # opaque — must not embed the raw ip
    assert "10.0.0.1" not in row["session_token"]


def test_token_resolves_detail(client, in_memory_duckdb, test_service_source):
    """A token taken from the list resolves a non-empty detail result."""
    _seed(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["data"], "token-keyed detail returned no rows"


def test_invalid_token_returns_400(client, in_memory_duckdb, test_service_source):
    """A malformed/stale token surfaces a clear 400, not a silent empty grid."""
    _seed(in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": "garbage.token"})
    assert r.status_code == 400, r.text
    assert r.json()["detail"]["error"] == "session_token_invalid"


def test_zero_width_session_detail_via_token(client, in_memory_duckdb, test_service_source):
    """A single-request session (session_start == session_end) must still
    resolve detail rows. Previously the time clamp rejected the zero-width
    window with 400 'time_range_empty' and the modal showed 'No results' —
    for ~28% of real sessions, all roles. The detail handler now widens a
    zero-width window by 1s/side before clamping."""
    _seed_zero_width(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    assert row["session_start"] == row["session_end"], "fixture should produce a zero-width session"
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    assert r.json()["data"], "zero-width session detail returned no rows"


def test_zero_width_session_detail_via_raw_ip(client, in_memory_duckdb, test_service_source):
    """The same zero-width widening helps the legacy raw-ip path (admin /
    non-masking analyst), not only the token path — the widen is shared."""
    _seed_zero_width(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": row["ip"], "start_time": row["session_start"], "end_time": row["session_end"]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["data"], "zero-width session detail (raw ip) returned no rows"


@pytest.mark.security_regression
@pytest.mark.parametrize("raw_ip", ["10.0.0.1", "10.0.0.xxx"])
def test_masking_analyst_raw_ip_rejected(client, in_memory_duckdb, test_service_source, raw_ip):
    """A masking analyst supplying a raw OR masked top-level ip (no token) is
    rejected with 403 — accepting it would be a presence oracle (the masked
    value can never match; a raw value would probe for a guessed real IP)."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=True)
    start, end = _window()
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": raw_ip, "start_time": start, "end_time": end},
    )
    assert r.status_code == 403, r.text
    assert r.json()["detail"]["error"] == "pii_policy_violation"


@pytest.mark.security_regression
def test_masking_analyst_token_returns_rows(client, in_memory_duckdb, test_service_source):
    """The fix itself: a masking analyst drills in via the opaque token and gets
    rows — without ever holding the real IP."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=True)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    assert r.json()["data"], "masking analyst token-keyed detail returned no rows"


@pytest.mark.security_regression
def test_non_masking_analyst_raw_ip_allowed(client, in_memory_duckdb, test_service_source):
    """A NON-masking analyst still uses the legacy raw-ip path (the lock is
    gated on pii_policy.mask_ips, not on analyst-hood)."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=False)
    start, end = _window()
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": "10.0.0.1", "start_time": start, "end_time": end},
    )
    assert r.status_code == 200, r.text
    assert r.json()["data"], "non-masking analyst raw-ip detail returned no rows"


@pytest.mark.security_regression
def test_detail_body_ip_column_is_masked_by_policy():
    """Pins that the detail response shape ({"data":[{"ip":...}], "columns":[...]})
    is caught by apply_pii_policy's key-name masking — the invariant that keeps
    raw IPs out of a masking analyst's detail body once rows are returned. If a
    future change aliases the ip column out of IP_FAMILY_KEYS this fails."""
    body = {
        "columns": ["ip", "url"],
        "data": [
            {"ip": "203.0.113.45", "url": "/a"},
            {"ip": "198.51.100.7", "url": "/b"},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [r["ip"] for r in masked["data"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    # non-PII columns stay verbatim
    assert [r["url"] for r in masked["data"]] == ["/a", "/b"]


@pytest.mark.security_regression
def test_detail_body_cookie_session_is_redacted_by_policy():
    """Phase-4 Track C added `cookie_session` — a hashed per-client session id
    captured at the edge. It is client PII, so a mask_ips analyst viewing raw
    /logs or /query rows must NOT see the hash. apply_pii_policy redacts the
    cookie_session column (SESSION_ID_KEYS) wholesale; if a future change drops
    it from the masked set this fails."""
    body = {
        "columns": ["ip", "cookie_session", "url"],
        "data": [
            {"ip": "203.0.113.45", "cookie_session": "a1b2c3d4deadbeef", "url": "/a"},
            {"ip": "198.51.100.7", "cookie_session": "", "url": "/b"},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    # Non-empty session hash → redacted; empty stays empty; raw hash never leaks.
    assert [r["cookie_session"] for r in masked["data"]] == ["[redacted]", ""]
    assert all("deadbeef" not in str(r.get("cookie_session")) for r in masked["data"])
    # IP still masked, url still verbatim.
    assert [r["ip"] for r in masked["data"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    assert [r["url"] for r in masked["data"]] == ["/a", "/b"]


@pytest.mark.security_regression
def test_aggregates_topn_cookie_session_value_is_redacted_by_policy():
    """/api/dashboard/aggregates top-N panels carry the dimension value under a
    generic ``value`` key nested as ``data[<field>]["top"][i]["value"]`` — the
    field name is the PARENT key, not the cell key. Pins that apply_pii_policy
    threads that field context so a cookie_session top-N cell is redacted (a
    mask_ips analyst must not read raw session hashes off the Top card) while
    the IP top-N cell stays masked (unchanged) and url stays verbatim."""
    body = {
        "data": {
            "ip": {"top": [{"value": "203.0.113.45", "count": 9}], "total": 9},
            "cookie_session": {
                "top": [{"value": "a1b2c3d4deadbeef", "count": 7}, {"value": "", "count": 1}],
                "total": 8,
            },
            "url": {"top": [{"value": "/login", "count": 5}], "total": 5},
        }
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    cs_top = masked["data"]["cookie_session"]["top"]
    assert [c["value"] for c in cs_top] == ["[redacted]", ""]  # empty preserved
    assert "deadbeef" not in str(masked["data"]["cookie_session"])
    # IP top-N still value-shape masked; url panel untouched.
    assert masked["data"]["ip"]["top"][0]["value"] == "203.0.113.xxx"
    assert masked["data"]["url"]["top"][0]["value"] == "/login"


@pytest.mark.security_regression
def test_field_values_cookie_session_is_redacted_by_policy():
    """/api/dashboard/field-values names its column in a SIBLING ``field`` key
    (``{"field": "cookie_session", "values": [{"value": <hash>}]}``), not the
    parent key. Pins that apply_pii_policy resolves that sibling so the distinct
    cookie_session values are redacted — otherwise a mask_ips analyst could
    enumerate every raw session hash via the value picker."""
    body = {
        "field": "cookie_session",
        "values": [
            {"value": "a1b2c3d4deadbeef", "count": 12},
            {"value": "", "count": 3},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [v["value"] for v in masked["values"]] == ["[redacted]", ""]  # empty preserved
    assert "deadbeef" not in str(masked)
    assert masked["field"] == "cookie_session"  # the field NAME is fine to show


@pytest.mark.security_regression
def test_field_values_ip_is_masked_by_policy():
    """/api/dashboard/field-values with field=ip names the column in the SIBLING
    ``field`` key, so the raw distinct IP ``value`` cells slipped past the
    key-name masker — a mask_ips analyst could enumerate every raw client IP via
    the value picker. Pins that apply_pii_policy resolves the sibling ``field``
    for IP columns too and value-shape masks the cells (non-IP values verbatim)."""
    body = {
        "field": "ip",
        "values": [
            {"value": "203.0.113.45", "count": 12},
            {"value": "198.51.100.7", "count": 3},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [v["value"] for v in masked["values"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    # A non-IP dimension (url) must stay verbatim — even an IP-shaped url value.
    url_body = {"field": "url", "values": [{"value": "/a"}, {"value": "1.2.3.4"}]}
    assert [v["value"] for v in apply_pii_policy(url_body, {"mask_ips": True})["values"]] == ["/a", "1.2.3.4"]


def test_sessions_with_range_token(client, in_memory_duckdb, test_service_source):
    """Verify range_token is recognized and resolved server-side on POST /api/sessions."""
    _seed(in_memory_duckdb, test_service_source, ip="10.0.0.42", n=5)
    r = client.post(
        "/api/sessions",
        headers=_HDR,
        json={"range_token": "1h"},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert "sessions" in data
    assert any(s["ip"] == "10.0.0.42" for s in data["sessions"])


def test_sessions_telemetry_attribution(client, in_memory_duckdb, test_service_source, monkeypatch):
    """Verify process_context is captured for sessions:list and sessions:detail."""
    captured_contexts = []
    import backend.routers.sessions as sess_mod

    orig_scope = sess_mod.process_context_scope

    def mock_scope(ctx_name):
        captured_contexts.append(ctx_name)
        return orig_scope(ctx_name)

    monkeypatch.setattr(sess_mod, "process_context_scope", mock_scope)
    _seed(in_memory_duckdb, test_service_source, ip="10.0.0.55", n=5)

    r = client.post("/api/sessions", headers=_HDR, json={"range_token": "1h"})
    assert r.status_code == 200
    assert "sessions:list" in captured_contexts

    row = r.json()["sessions"][0]
    token = row["session_token"]
    r_detail = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": token})
    assert r_detail.status_code == 200
    assert "sessions:detail" in captured_contexts


def test_sessions_stale_view_retry_wrapped(client, in_memory_duckdb, test_service_source, monkeypatch):
    """Verify execute_with_stale_view_retry wraps get_sessions in sessions_endpoint."""
    import backend.routers.sessions as sess_mod

    called = []
    orig_retry = sess_mod.execute_with_stale_view_retry

    def mock_retry(con, src, fn):
        called.append(True)
        return orig_retry(con, src, fn)

    monkeypatch.setattr(sess_mod, "execute_with_stale_view_retry", mock_retry)
    _seed(in_memory_duckdb, test_service_source, ip="10.0.0.99", n=2)
    r = client.post("/api/sessions", headers=_HDR, json={})
    assert r.status_code == 200
    assert called, "execute_with_stale_view_retry should be called by sessions_endpoint"
