"""Tests for dynamic RUM tracker, Faro SDK, and direct backend beacon ingestion routes."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from backend.main import app

SVC = "TestRumAssetsSvc"


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def with_config(monkeypatch):
    container: dict = {}

    def fake_load(svc_id):
        return container.get(svc_id)

    def fake_get_active(fallback_to_first=True):
        return list(container.keys())[0] if container else SVC

    monkeypatch.setattr("backend.config.load_config", fake_load)
    monkeypatch.setattr("backend.config.get_active_service_id", fake_get_active)
    return container


def test_get_rum_tracker_js(client, with_config, monkeypatch):
    # Set up config
    with_config[SVC] = {"service_id": SVC, "rum": {"faro_version": "2.9.0"}}

    # Mock generate_rum_tracker_js
    fake_js = "console.log('rum tracker code');"
    monkeypatch.setattr("backend.provision.rum_assets.generate_rum_tracker_js", lambda sid: f"{fake_js} /* {sid} */")

    # 1. No headers/cookies -> defaults to active service
    r = client.get("/js/rum.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert f"{fake_js} /* {SVC} */" in r.text

    # 2. Service ID in query param
    r = client.get("/js/rum.js?service_id=CustomSvc")
    # CustomSvc not loaded in with_config, so it should fall back to first active
    assert r.status_code == 200
    assert f"{fake_js} /* {SVC} */" in r.text

    # With CustomSvc in config
    with_config["CustomSvc"] = {"service_id": "CustomSvc"}
    r = client.get("/js/rum.js?service_id=CustomSvc")
    assert r.status_code == 200
    assert f"{fake_js} /* CustomSvc */" in r.text


def test_get_faro_sdk_js(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "rum": {"faro_version": "2.9.0"}}

    async def mock_fetch(version):
        return f"console.log('faro bundle version {version}');".encode()

    monkeypatch.setattr("backend.core.faro_versions.fetch_faro_bundle", mock_fetch)

    r = client.get("/js/faro-sdk.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert "console.log('faro bundle version 2.9.0');" in r.text
