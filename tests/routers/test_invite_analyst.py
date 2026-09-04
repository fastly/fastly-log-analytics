"""Tests for the generate-viewer-key endpoint and the simplified invite flow."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app

_FAKE_CFG = {
    "service_id": "svc123",
    "name": "Test Service",
    "access_level": "read_write",
    "fos_bucket": "test-bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
    "fos_prefix": "",
    "fos_access_key_id": "AKID",
    "fos_secret_access_key": "SECRET",
    "fastly_api_key": "stored-admin-token",
}

_FAKE_KEY = {"access_key": "ANALYST_KEY", "secret_key": "ANALYST_SECRET"}


def test_generate_viewer_key_returns_raw_credentials():
    """Viewer-key endpoint uses stored fastly_api_key and returns credentials."""
    with (
        patch("backend.config.load_config", return_value=_FAKE_CFG),
        patch("backend.provision.orchestrator.fastly", return_value=_FAKE_KEY),
        patch("backend.config.config_to_source", return_value={**_FAKE_CFG}),
        patch("backend.core.iceberg._get_catalog", side_effect=Exception("no catalog")),
    ):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 200
    data = response.json()
    assert data["access_key_id"] == "ANALYST_KEY"
    assert data["secret_key"] == "ANALYST_SECRET"
    assert data["fos_bucket"] == "test-bucket"
    # Encrypted invite fields must not be present
    assert "invite_block" not in data
    assert "invite_secret" not in data
    assert "expires_at" not in data


def test_generate_viewer_key_requires_stored_api_key():
    """Missing fastly_api_key in config returns 400."""
    cfg_no_token = {**_FAKE_CFG, "fastly_api_key": ""}
    with patch("backend.config.load_config", return_value=cfg_no_token):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 400


def test_generate_viewer_key_rejects_celery_topology():
    """Scalable DuckLake services must use Path B instead of FOS-only invites."""
    with (
        patch("backend.config.INGEST_MODE", "celery"),
        patch("backend.config.load_config", return_value=_FAKE_CFG),
    ):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 409
    assert "Path B" in response.json()["detail"]["error"]


def test_generate_viewer_key_requires_read_write_service():
    """Analyst services (read_only) cannot generate viewer keys."""
    ro_cfg = {**_FAKE_CFG, "access_level": "read_only"}
    with patch("backend.config.load_config", return_value=ro_cfg):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 403


def test_decrypt_invite_endpoint_does_not_exist():
    """/api/provision/decrypt-invite is not a valid endpoint."""
    client = TestClient(app)
    response = client.post(
        "/api/provision/decrypt-invite",
        json={"invite_block": "x", "secret_key": "y"},
    )
    assert response.status_code == 404 or response.status_code == 405
