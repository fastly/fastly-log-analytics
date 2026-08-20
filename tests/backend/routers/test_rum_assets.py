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


def test_post_rum_beacon_vitals(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC}

    # Mock source retrieval
    monkeypatch.setattr(
        "backend.routers.rum.get_source_for_service", lambda sid: {"service_id": sid, "bucket": "b", "prefix": "p"}
    )

    # Capture written buffer
    captured = []

    def fake_write_to_buffer(source, table, filename, table_name):
        captured.append({"source": source, "table": table, "filename": filename, "table_name": table_name})
        return filename

    monkeypatch.setattr("backend.core.iceberg.write_to_buffer", fake_write_to_buffer)

    # Send a vitals beacon
    r = client.post(
        "/rum-beacon?service_id=TestRumAssetsSvc&rum_metric_name=LCP&rum_metric_value=2500&rum_metric_rating=poor&cid=test-cid"
    )
    assert r.status_code == 204

    assert len(captured) == 1
    item = captured[0]
    assert item["table_name"] == "client_vitals"
    assert item["source"]["service_id"] == SVC

    # Verify the row data
    t = item["table"].to_pylist()
    assert len(t) == 1
    assert t[0]["metric_name"] == "LCP"
    assert t[0]["metric_value"] == 2500.0
    assert t[0]["metric_rating"] == "poor"
    assert t[0]["cid"] == "test-cid"


def test_post_rum_beacon_errors(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC}
    monkeypatch.setattr(
        "backend.routers.rum.get_source_for_service", lambda sid: {"service_id": sid, "bucket": "b", "prefix": "p"}
    )

    captured = []

    def fake_write_to_buffer(source, table, filename, table_name):
        captured.append({"source": source, "table": table, "filename": filename, "table_name": table_name})
        return filename

    monkeypatch.setattr("backend.core.iceberg.write_to_buffer", fake_write_to_buffer)

    # Send an error beacon
    r = client.get(
        "/rum-beacon?service_id=TestRumAssetsSvc&rum_error_message=Failed%20to%20load&rum_error_file=app.js&rum_error_line=42&rum_error_col=10"
    )
    assert r.status_code == 204

    assert len(captured) == 1
    item = captured[0]
    assert item["table_name"] == "client_errors"

    # Verify the row data
    t = item["table"].to_pylist()
    assert len(t) == 1
    assert t[0]["error_message"] == "Failed to load"
    assert t[0]["error_file"] == "app.js"
    assert t[0]["error_line"] == 42
    assert t[0]["error_col"] == 10
