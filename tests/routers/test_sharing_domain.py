"""Tests for the sharing_domain router."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.main import app


@pytest.fixture
def client():
    return TestClient(app)


def clean_res(d: dict) -> dict:
    """Helper to strip telemetry/debug fields added by middleware."""
    return {k: v for k, v in d.items() if not k.startswith("_")}


def test_deploy_frontend_with_token_override(client):
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

    with patch("backend.routers.sharing_domain.deploy_remote_frontend", return_value=mock_res) as mock_deploy:
        response = client.post("/api/sharing/deploy-frontend", json=payload)
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


def test_deploy_frontend_resolves_token_from_config(client):
    """Verify that token is resolved from list_configs() when token_override is not provided."""
    payload = {
        "service_name": "test-config-service",
        "domain_name": "config-domain.global.ssl.fastly.net",
    }

    mock_res = {
        "service_id": "service_config",
        "version": 2,
        "domain_name": "config-domain.global.ssl.fastly.net",
        "origin_host": "34.123.30.195",
    }

    mock_configs = [
        {"service_id": "some-other-id", "fastly_api_key": ""},
        {"service_id": "target-id", "fastly_api_key": "config-resolved-token"},
    ]

    with (
        patch("backend.routers.sharing_domain.list_configs", return_value=mock_configs),
        patch("backend.routers.sharing_domain.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
    ):
        response = client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_deploy.assert_called_once_with(
            service_name="test-config-service",
            domain_name="config-domain.global.ssl.fastly.net",
            origin_host="34.123.30.195",
            origin_port=80,
            use_ssl=False,
            token="config-resolved-token",
            override_host=None,
        )


def test_deploy_frontend_resolves_token_from_env(client):
    """Verify that token falls back to FASTLY_API_KEY environment variable if not in override or configs."""
    payload = {
        "service_name": "test-env-service",
        "domain_name": "env-domain.global.ssl.fastly.net",
    }

    mock_res = {
        "service_id": "service_env",
        "version": 3,
        "domain_name": "env-domain.global.ssl.fastly.net",
        "origin_host": "34.123.30.195",
    }

    with (
        patch("backend.routers.sharing_domain.list_configs", return_value=[]),
        patch.dict(os.environ, {"FASTLY_API_KEY": "env-resolved-token"}),
        patch("backend.routers.sharing_domain.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
    ):
        response = client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_deploy.assert_called_once_with(
            service_name="test-env-service",
            domain_name="env-domain.global.ssl.fastly.net",
            origin_host="34.123.30.195",
            origin_port=80,
            use_ssl=False,
            token="env-resolved-token",
            override_host=None,
        )


def test_deploy_frontend_missing_token_error(client):
    """Verify that a 400 error is returned when no token can be resolved."""
    payload = {
        "service_name": "test-fail-service",
        "domain_name": "fail-domain.global.ssl.fastly.net",
    }

    with (
        patch("backend.routers.sharing_domain.list_configs", return_value=[]),
        patch.dict(os.environ, {}),
    ):
        # Temporarily ensure FASTLY_API_KEY is not in env if it's there
        if "FASTLY_API_KEY" in os.environ:
            with patch.dict(os.environ, {}, clear=True):
                response = client.post("/api/sharing/deploy-frontend", json=payload)
        else:
            response = client.post("/api/sharing/deploy-frontend", json=payload)

        assert response.status_code == 400
        assert "Token is required" in response.json()["detail"]["message"]


def test_deploy_frontend_fails_500(client):
    """Verify that a 500 error is returned when the deployment process raises an exception."""
    payload = {
        "service_name": "test-error-service",
        "domain_name": "error-domain.global.ssl.fastly.net",
        "token_override": "some-token",
    }

    with patch(
        "backend.routers.sharing_domain.deploy_remote_frontend",
        side_effect=RuntimeError("Fastly API error during Create Service: HTTP 400 - Name already taken"),
    ):
        response = client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "request_failed"
        assert "error_id" in response.json()["detail"]


def test_deploy_frontend_with_override_host(client):
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

    with patch("backend.routers.sharing_domain.deploy_remote_frontend", return_value=mock_res) as mock_deploy:
        response = client.post("/api/sharing/deploy-frontend", json=payload)
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


def test_teardown_frontend_success(client):
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
            "origin_host": "34.123.30.195",
        },
    }

    with (
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config) as mock_load,
        patch("backend.routers.sharing_domain.delete_remote_frontend") as mock_delete,
        patch("backend.config.save_config") as mock_save,
    ):
        response = client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        assert response.json()["ok"] is True
        mock_load.assert_called_once_with("service123")
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="teardown-token")
        assert "remote_frontend" not in mock_config
        mock_save.assert_called_once_with("service123", mock_config)


def test_teardown_frontend_already_removed(client):
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
        patch("backend.routers.sharing_domain.delete_remote_frontend") as mock_delete,
        patch("backend.config.save_config") as mock_save,
    ):
        response = client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        assert response.json()["ok"] is True
        mock_delete.assert_not_called()
        mock_save.assert_not_called()


def test_deploy_frontend_with_service_id_writes_config(client):
    """Verify that deploy-frontend writes remote_frontend block to service config when service_id is supplied."""
    payload = {
        "service_name": "test-service",
        "domain_name": "test-domain.global.ssl.fastly.net",
        "token_override": "token-123",
        "service_id": "logging-service-123",
    }

    mock_res = {
        "service_id": "service_remote_id",
        "version": 1,
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "34.123.30.195",
    }

    mock_config = {
        "service_id": "logging-service-123",
    }

    with (
        patch("backend.routers.sharing_domain.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
        patch("backend.utils.router_utils.load_service_config", return_value=mock_config) as mock_load,
        patch("backend.config.save_config") as mock_save,
    ):
        response = client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res
        mock_load.assert_called_once_with("logging-service-123")
        assert mock_config["remote_frontend"] == {
            "service_id": "service_remote_id",
            "version": 1,
            "domain_name": "test-domain.global.ssl.fastly.net",
            "origin_host": "34.123.30.195",
        }
        mock_save.assert_called_once_with("logging-service-123", mock_config)


def test_deploy_frontend_with_service_id_save_config_error_graceful(client):
    """Verify that deploy-frontend handles config saving exceptions gracefully and still returns the deployment result."""
    payload = {
        "service_name": "test-service",
        "domain_name": "test-domain.global.ssl.fastly.net",
        "token_override": "token-123",
        "service_id": "logging-service-123",
    }

    mock_res = {
        "service_id": "service_remote_id",
        "version": 1,
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "34.123.30.195",
    }

    with (
        patch("backend.routers.sharing_domain.deploy_remote_frontend", return_value=mock_res),
        patch("backend.utils.router_utils.load_service_config", side_effect=RuntimeError("filesystem full")),
    ):
        response = client.post("/api/sharing/deploy-frontend", json=payload)
        assert response.status_code == 200
        assert clean_res(response.json()) == mock_res


def test_teardown_frontend_resolves_token_from_config(client):
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
        patch("backend.routers.sharing_domain.list_configs", return_value=mock_configs),
        patch("backend.routers.sharing_domain.delete_remote_frontend") as mock_delete,
        patch("backend.config.save_config"),
    ):
        response = client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="config-teardown-token")


def test_teardown_frontend_resolves_token_from_env(client):
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
        patch("backend.routers.sharing_domain.list_configs", return_value=[]),
        patch.dict(os.environ, {"FASTLY_API_KEY": "env-teardown-token"}),
        patch("backend.routers.sharing_domain.delete_remote_frontend") as mock_delete,
        patch("backend.config.save_config"),
    ):
        response = client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 200
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="env-teardown-token")


def test_teardown_frontend_missing_token_error(client):
    """Verify that teardown-frontend returns 400 when no token can be resolved."""
    payload = {
        "service_id": "service123",
    }

    with (
        patch("backend.routers.sharing_domain.list_configs", return_value=[]),
        patch.dict(os.environ, {}, clear=True),
    ):
        if "FASTLY_API_KEY" in os.environ:
            with patch.dict(os.environ, {}, clear=True):
                response = client.post("/api/sharing/teardown-frontend", json=payload)
        else:
            response = client.post("/api/sharing/teardown-frontend", json=payload)

        assert response.status_code == 400
        assert "Token is required" in response.json()["detail"]["message"]


def test_teardown_frontend_fails_500(client):
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
        patch("backend.routers.sharing_domain.delete_remote_frontend", side_effect=RuntimeError("fastly offline")),
    ):
        response = client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 500
        assert response.json()["detail"]["error"] == "request_failed"


def test_teardown_frontend_http_exception_passed_through(client):
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
            "backend.routers.sharing_domain.delete_remote_frontend",
            side_effect=HTTPException(status_code=403, detail="Forbidden"),
        ),
    ):
        response = client.post("/api/sharing/teardown-frontend", json=payload)
        assert response.status_code == 403
