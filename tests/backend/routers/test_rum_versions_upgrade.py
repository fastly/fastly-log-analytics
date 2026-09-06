"""Tests for backend.routers.rum — /rum/versions and /rum/upgrade.

Mocks the npm-registry lookup (``fetch_available_faro_versions``) and the
orchestrator (``run_with_events`` / ``upgrade_faro_version``) at the
module-attribute level rather than the network layer — no test here may
make a real network call to npm, FOS, or the Fastly API.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.provision.rum_orchestrator_v2 import upgrade_faro_version

SVC = "TestRumVersionsUpgradeSvc"


@pytest.fixture
def client():
    """Plain TestClient(app) — mirrors test_session_scoring_router.py's
    rationale: these tests mock ``backend.config.load_config`` directly and
    don't need the conftest DB-override plumbing."""
    return TestClient(app)


@pytest.fixture
def with_config(monkeypatch):
    container: dict = {}

    def fake_load(svc_id):
        return container.get(svc_id)

    monkeypatch.setattr("backend.config.load_config", fake_load)
    return container


# ── GET /rum/versions ────────────────────────────────────────────────────


def test_versions_update_available(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "rum": {"faro_version": "2.8.2"}}

    async def fake_fetch():
        return ["2.9.0", "2.8.2", "2.8.1"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{SVC}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] == ["2.9.0", "2.8.2", "2.8.1"]
    assert body["current"] == "2.8.2"
    assert body["latest"] == "2.9.0"
    assert body["update_available"] is True


def test_versions_already_up_to_date(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "rum": {"faro_version": "2.9.0"}}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{SVC}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["current"] == "2.9.0"
    assert body["latest"] == "2.9.0"
    assert body["update_available"] is False


def test_versions_unpinned_service(client, with_config, monkeypatch):
    """No ``rum.faro_version`` in config (never enabled / never upgraded) ->
    current is null and update_available is False even though a latest
    version is known — there's nothing to compare against yet."""
    with_config[SVC] = {"service_id": SVC}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{SVC}/rum/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["current"] is None
    assert body["latest"] == "2.9.0"
    assert body["update_available"] is False


def test_versions_registry_failure_returns_503(client, with_config, monkeypatch):
    """A raw ValueError from fetch_available_faro_versions (registry down /
    rate-limited / malformed payload) must surface as a deliberate 503, not
    an unhandled 500 and not a silently-empty 200."""
    with_config[SVC] = {"service_id": SVC, "rum": {"faro_version": "2.8.2"}}

    async def fake_fetch():
        raise ValueError("Failed to fetch Faro versions: registry returned 503")

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    r = client.get(f"/api/services/{SVC}/rum/versions")
    assert r.status_code == 503
    assert r.json()["detail"]["error"] == "faro_registry_unavailable"


# ── POST /rum/upgrade ────────────────────────────────────────────────────


def test_upgrade_rejects_unknown_version_before_orchestration(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    mock_run_with_events = MagicMock()
    monkeypatch.setattr("backend.routers.rum.run_with_events", mock_run_with_events)

    r = client.post(f"/api/services/{SVC}/rum/upgrade", json={"version": "9.9.9"})
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "unknown_faro_version"
    mock_run_with_events.assert_not_called()


def test_upgrade_happy_path_invokes_orchestrator_and_streams_events(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

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
        with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN", "rum": {"faro_version": "2.9.0"}}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{SVC}/rum/upgrade", json={"version": "2.9.0"})

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
    assert captured["args"] == (SVC, "2.9.0", "TOKEN")
    assert captured["kwargs"]["activate"] is True


def test_upgrade_streams_error_event_on_orchestrator_failure(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    async def fake_fetch():
        return ["2.9.0", "2.8.2"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "downloading"}
        raise RuntimeError("upload failed: FOS 503")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{SVC}/rum/upgrade", json={"version": "2.9.0"})

    assert r.status_code == 200  # streaming endpoint always 200; error rides the body
    events = [json.loads(line[len("data: ") :]) for line in r.text.splitlines() if line.startswith("data: ")]
    types = [e["type"] for e in events]
    assert "error" in types
    assert "done" not in types
    err = next(e for e in events if e["type"] == "error")
    assert "fos" in err["message"].lower()


def test_upgrade_400_when_no_token_anywhere(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": ""}

    async def fake_fetch():
        return ["2.9.0"]

    monkeypatch.setattr("backend.routers.rum.fetch_available_faro_versions", fake_fetch)

    r = client.post(f"/api/services/{SVC}/rum/upgrade", json={"version": "2.9.0"})
    assert r.status_code == 400
    assert "api key" in r.json()["detail"]["error"].lower()
