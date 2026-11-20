"""Security regression and functional tests for RUM provisioning routes (SEC-13 / SEC-15)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.routers.rum.provisioning import rum_provisioning_router

pytestmark = pytest.mark.no_postgres

SVC = "svc-rum-sec-test"


def _make_rum_app(*, analyst: bool = False) -> FastAPI:
    sub_app = FastAPI()

    @sub_app.middleware("http")
    async def _inject_state(request: Request, call_next):
        request.state.analyst_session = (
            SimpleNamespace(session_id="sess-analyst", service_ids=[SVC], email="a@example.com") if analyst else None
        )
        return await call_next(request)

    sub_app.include_router(rum_provisioning_router, prefix="/api/services")
    return sub_app


def _settings_payload(custom_condition: str) -> dict[str, object]:
    return {
        "capture_vitals": True,
        "capture_performance": True,
        "capture_errors": True,
        "capture_events": True,
        "custom_condition": custom_condition,
    }


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "malicious_condition",
    [
        "true) || (true",
        "((true)",
        "(true))",
        'req.http.Host == "unterminated',
        'req.http.Host == "ok"\\',
        "req.http.Host == 'single'",
        'true; set req.http.X-Pwn = "1"',
        "true\n|| true",
        "true\r|| true",
        "true\x00",
        'req.http.Host == {"long"}',
        "true // inline comment",
        "true /* block comment */",
        "true # hash comment",
        'req.http.Host == "${file(\\"/etc/passwd\\")}"',
        'req.http.Host == "%{if true}x%{endif}"',
        'req.http.Host == "}V"',
        'table.lookup(fos_credentials, "secret_key", "") != ""',
        'TABLE . LOOKUP(cdn_auth, "secret_key", "") != ""',
        'req.http.Authorization != ""',
        'req.http.X-Edge-Scorer-Auth != ""',
        'req.http.x-fastly-key != ""',
        'req.http.x-flp-front-key != ""',
        'req.http.X-Probe == "session_scoring_secrets"',
        r'req.http.X-A == "\") || true || (req.http.X-B == "x" && req.http.X-C == "y\"',
        r"req.http.Host == \"ok\"",
    ],
)
def test_sec15_rum_settings_rejects_vcl_injection_and_secret_exfiltration(malicious_condition: str) -> None:
    client = TestClient(_make_rum_app(analyst=False))
    cfg = {
        "fastly_api_key": "tok-stored",
        "rum_enabled": True,
        "rum": {"enabled": True, "custom_condition": ""},
    }
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config") as mock_mutate,
        patch("backend.provision.rum_assets.upload_rum_tracker_js") as mock_upload,
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile,
    ):
        res = client.post(f"/api/services/{SVC}/rum/settings", json=_settings_payload(malicious_condition))

    assert res.status_code == 400, f"Expected 400 for {malicious_condition!r}, got {res.status_code}: {res.text}"
    detail = res.json()["detail"]
    assert detail["error"] == "invalid_rum_condition"
    mock_mutate.assert_not_called()
    mock_upload.assert_not_called()
    mock_reconcile.assert_not_called()


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "valid_condition",
    [
        "",
        'req.http.Host == "example.com"',
        'req.url !~ "^/healthz"',
        'req.http.User-Agent !~ "bot\\b" && (client.geo.country_code == "US")',
    ],
)
def test_sec15_rum_settings_allows_valid_conditions(valid_condition: str) -> None:
    client = TestClient(_make_rum_app(analyst=False))
    cfg = {
        "fastly_api_key": "tok-stored",
        "rum_enabled": True,
        "rum": {"enabled": True, "custom_condition": ""},
    }

    def _apply_mutation(_sid: str, fn):
        fn(cfg)
        return cfg

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=_apply_mutation) as mock_mutate,
        patch("backend.provision.rum_assets.upload_rum_tracker_js", return_value={"skipped": False}) as mock_upload,
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state", return_value=MagicMock(error=None)),
    ):
        res = client.post(f"/api/services/{SVC}/rum/settings", json=_settings_payload(valid_condition))

    assert res.status_code == 200, res.text
    assert res.json()["ok"] is True
    assert mock_mutate.called
    mock_upload.assert_called_once_with(SVC, "tok-stored", overwrite=True)

    # Also verify unmocked FeatureState.from_config and generate_terraform succeed on the persisted condition
    from backend.provision.declarative.models import FeatureState
    from backend.utils.terraform_gen import generate_terraform

    full_cfg = {
        "service_id": SVC,
        "logging_service_id": SVC,
        "log_period": 60,
        "sample_rate": 100,
        "fos_bucket": "rum-test-bkt",
        "fos_bucket_name": "rum-test-bkt",
        "fos_region": "us-east-1",
        "cdn_secret": "cdn-sec",
        **cfg,
    }
    state = FeatureState.from_config(full_cfg)
    assert state.rum_custom_condition == valid_condition
    tf_out = generate_terraform(full_cfg, "AKIA_TEST", "sec_test")
    assert "logging_service.tf.json" in tf_out


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (f"/api/services/{SVC}/rum/settings", _settings_payload("")),
        (f"/api/services/{SVC}/rum/enable", {"token": "tok-test", "activate": True}),
        (f"/api/services/{SVC}/rum/disable", {"token": "tok-test", "activate": True}),
        (f"/api/services/{SVC}/rum/upgrade", {"token": "tok-test", "version": "2.9.0"}),
    ],
)
def test_sec13_rum_admin_endpoints_reject_analyst_sessions(path: str, payload: dict[str, object]) -> None:
    analyst_client = TestClient(_make_rum_app(analyst=True))
    res = analyst_client.post(path, json=payload)
    assert res.status_code == 403
    assert res.json()["detail"]["error"] == "admin_only"


@pytest.mark.security_regression
def test_sec13_enable_rum_defers_fastly_api_key_persist_until_enable_succeeds() -> None:
    client = TestClient(_make_rum_app(analyst=False))
    cfg: dict[str, Any] = {"service_id": SVC, "fastly_api_key": "tok-stored", "rum_enabled": False}

    def _apply_mutation(_sid: str, fn):
        fn(cfg)
        return cfg

    def _failing_events(_func, *_args, **_kwargs):
        yield {"type": "status", "message": "starting"}
        raise RuntimeError("Fastly API error 403: Forbidden")

    with (
        patch("backend.config.load_config", side_effect=lambda _sid: cfg),
        patch("backend.config.mutate_config", side_effect=_apply_mutation) as mock_mutate,
        patch(
            "backend.routers.rum.provisioning.fetch_available_faro_versions",
            new=AsyncMock(return_value=["2.9.0"]),
        ),
        patch("backend.routers.rum.provisioning.run_with_events", side_effect=_failing_events),
    ):
        res_fail = client.post(f"/api/services/{SVC}/rum/enable", json={"token": "tok-bad-new", "activate": True})

    assert res_fail.status_code == 200
    assert "403" in res_fail.text
    mock_mutate.assert_not_called()
    assert cfg["fastly_api_key"] == "tok-stored"

    def _ok_events(_func, *_args, **_kwargs):
        # Before enable_rum finishes, stored key must still be untouched
        assert cfg["fastly_api_key"] == "tok-stored"
        yield {"type": "status", "message": "enabled"}

    with (
        patch("backend.config.load_config", side_effect=lambda _sid: cfg),
        patch("backend.config.mutate_config", side_effect=_apply_mutation) as mock_mutate_ok,
        patch(
            "backend.routers.rum.provisioning.fetch_available_faro_versions",
            new=AsyncMock(return_value=["2.9.0"]),
        ),
        patch("backend.routers.rum.provisioning.run_with_events", side_effect=_ok_events),
        patch("backend.cron.scheduler.get_scheduler", return_value=SimpleNamespace(reload=lambda: None)),
    ):
        res_ok = client.post(f"/api/services/{SVC}/rum/enable", json={"token": "tok-good-new", "activate": True})

    assert res_ok.status_code == 200
    assert '"type": "done"' in res_ok.text or '"type":"done"' in res_ok.text
    mock_mutate_ok.assert_called_once()
    assert cfg["fastly_api_key"] == "tok-good-new"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("method", "suffix", "payload"),
    [
        ("POST", "enable", {"activate": True}),
        ("POST", "disable", {"activate": True}),
        ("GET", "versions", None),
        ("POST", "upgrade", {"version": "2.9.0", "activate": True}),
        ("POST", "settings", _settings_payload("")),
    ],
)
def test_sec13_rum_handlers_enforce_require_service_in_scope(
    method: str, suffix: str, payload: dict[str, object] | None
) -> None:
    from backend.core.request_context import require_admin

    scoped_app = _make_rum_app(analyst=True)
    scoped_app.dependency_overrides[require_admin] = lambda: None
    scoped_client = TestClient(scoped_app)

    other_svc = "svc-unauthorized-other"
    path = f"/api/services/{other_svc}/rum/{suffix}"
    res = scoped_client.get(path) if method == "GET" else scoped_client.post(path, json=payload)
    assert res.status_code == 403
    assert res.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_sec13_rum_upgrade_and_settings_validate_explicit_caller_token() -> None:
    client = TestClient(_make_rum_app(analyst=False))
    cfg = {
        "service_id": SVC,
        "fastly_api_key": "tok-stored",
        "rum_enabled": True,
        "rum": {"enabled": True, "custom_condition": ""},
    }

    def _ro_fastly(method: str, path: str, *, token: str, **_kw):
        assert path == "/tokens/self"
        assert token == "tok-readonly"
        return {"id": "tok_ro", "user_id": "u1", "scope": "global:read", "services": []}

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config") as mock_mutate,
        patch("backend.core.vcl_generator.fastly", side_effect=_ro_fastly),
        patch("backend.provision.rum_assets.upload_rum_tracker_js") as mock_upload,
        patch("backend.routers.rum.provisioning.run_with_events") as mock_run,
    ):
        res_upg = client.post(
            f"/api/services/{SVC}/rum/upgrade",
            json={"version": "2.9.0", "token": "tok-readonly", "activate": True},
        )
        assert res_upg.status_code == 401
        assert res_upg.json()["detail"]["error"] == "insufficient_scope"
        mock_run.assert_not_called()

        settings_body = {**_settings_payload(""), "token": "tok-readonly"}
        res_set = client.post(f"/api/services/{SVC}/rum/settings", json=settings_body)
        assert res_set.status_code == 401
        assert res_set.json()["detail"]["error"] == "insufficient_scope"
        mock_mutate.assert_not_called()
        mock_upload.assert_not_called()
