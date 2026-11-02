from __future__ import annotations

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("flag", ["FASTLY_MOCK_MODE", "FASTLY_API_MOCK", "OAUTH_MOCK_IDP"])
def test_production_rejects_mock_modes_at_startup(flag: str) -> None:
    env = {
        **os.environ,
        "FLA_PRODUCTION": "1",
        "FASTLY_MOCK_MODE": "0",
        "FASTLY_API_MOCK": "0",
        "OAUTH_MOCK_IDP": "0",
        flag: "1",
    }
    result = subprocess.run(
        [sys.executable, "-c", "import backend.main"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert f"{flag} must not be enabled in production" in result.stderr


@pytest.mark.parametrize("flag", ["FASTLY_MOCK_MODE", "FASTLY_API_MOCK", "OAUTH_MOCK_IDP"])
@pytest.mark.parametrize("truthy_val", ["1", "true", "True", " yes ", "ON"])
@pytest.mark.parametrize("production_val", ["0", "1", "true"])
def test_mock_modes_are_test_only(
    monkeypatch: pytest.MonkeyPatch, flag: str, truthy_val: str, production_val: str
) -> None:
    from backend.core.fastly.client import is_mock_mode
    from backend.routers.share_admin.tunnel_routes import mock_idp_enabled

    monkeypatch.setenv("FLA_PRODUCTION", production_val)
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    monkeypatch.delenv("FASTLY_API_MOCK", raising=False)
    monkeypatch.delenv("OAUTH_MOCK_IDP", raising=False)
    monkeypatch.setenv(flag, truthy_val)
    enabled = is_mock_mode if flag in ("FASTLY_MOCK_MODE", "FASTLY_API_MOCK") else mock_idp_enabled
    if production_val in ("1", "true"):
        with pytest.raises(RuntimeError, match=f"{flag} must not be enabled in production"):
            enabled()
    else:
        assert enabled() is True


def test_production_with_mock_modes_disabled_is_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils.runtime_modes import validate_mock_modes

    monkeypatch.setenv("FLA_PRODUCTION", "1")
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    monkeypatch.delenv("FASTLY_API_MOCK", raising=False)
    monkeypatch.delenv("OAUTH_MOCK_IDP", raising=False)
    validate_mock_modes()


def test_celery_and_high_scale_workers_validate_mock_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.celery_app import _worker_process_init
    from backend.high_scale.worker import build_worker_from_environment

    monkeypatch.setenv("FLA_PRODUCTION", "true")
    monkeypatch.setenv("FASTLY_API_MOCK", "true")

    with pytest.raises(RuntimeError, match="FASTLY_API_MOCK must not be enabled in production"):
        _worker_process_init()

    with pytest.raises(RuntimeError, match="FASTLY_API_MOCK must not be enabled in production"):
        build_worker_from_environment()


def test_rum_assets_mock_bucket_requires_explicit_mock_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    import httpx

    from backend.provision import rum_assets

    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    monkeypatch.delenv("FASTLY_API_MOCK", raising=False)
    monkeypatch.setenv("FLA_PRODUCTION", "0")

    cfg = {
        "fos_bucket": "mock_bucket",
        "fos_access_key_id": "ak",
        "fos_secret_access_key": "sk",
        "fos_region": "us-east-1",
        "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": "deadbeef"},
    }
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda _sid: cfg)
    monkeypatch.setattr(rum_assets, "faro_tracker_ready", lambda _cfg: (True, ""))

    put_called = []

    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "PUT":
            put_called.append(str(req.url))
            return httpx.Response(200)
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client
    monkeypatch.setattr(rum_assets, "make_client", lambda: real_client(transport=transport))

    rum_assets.upload_rum_tracker_js("svc_test", "tok")
    assert len(put_called) == 1, "mock_bucket without mock mode enabled must not bypass real PUT upload"

    put_called.clear()
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    rum_assets.upload_rum_tracker_js("svc_test", "tok")
    assert put_called == [], "mock_bucket with FASTLY_MOCK_MODE=1 should take the mock upload branch"
