"""Tests for backend.routers.rum — POST /rum/enable and POST /rum/disable.

Mirrors tests/backend/routers/test_rum_versions_upgrade.py: mocks
``run_with_events`` at the module-attribute level (no real orchestration /
Fastly API / FOS calls) and drives the SSE stream through TestClient.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from backend.main import app

SVC = "TestRumEnableDisableSvc"


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def with_config(monkeypatch):
    container: dict = {}

    def fake_load(svc_id):
        return container.get(svc_id)

    saved: list[dict] = []

    def fake_save(svc_id, cfg):
        container[svc_id] = cfg
        saved.append(dict(cfg))

    monkeypatch.setattr("backend.config.load_config", fake_load)
    monkeypatch.setattr("backend.config.save_config", fake_save)
    return container


def _events(response) -> list[dict]:
    return [json.loads(line[len("data: ") :]) for line in response.text.splitlines() if line.startswith("data: ")]


# ── POST /rum/enable ─────────────────────────────────────────────────────


def test_enable_404_when_service_has_no_config(client, with_config):
    r = client.post(f"/api/services/{SVC}/rum/enable", json={})
    assert r.status_code == 404


def test_enable_400_when_no_token_anywhere(client, with_config):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": ""}
    r = client.post(f"/api/services/{SVC}/rum/enable", json={})
    assert r.status_code == 400
    assert "token" in r.json()["detail"]["error"].lower()


def test_enable_persists_a_freshly_supplied_token(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": ""}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{SVC}/rum/enable", json={"token": "NEW-TOKEN"})
    assert r.status_code == 200
    assert with_config[SVC]["fastly_api_key"] == "NEW-TOKEN"


def test_enable_happy_path_streams_done_with_activate_message(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    captured: dict = {}

    def fake_run_with_events(func, *args, **kwargs):
        captured["func"] = func
        captured["kwargs"] = kwargs
        yield {"type": "status", "message": "enabling RUM"}
        with_config[SVC] = {
            "service_id": SVC,
            "fastly_api_key": "TOKEN",
            "rum_enabled": True,
            "rum_enabled_at": "2026-08-09T00:00:00Z",
        }

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{SVC}/rum/enable", json={"activate": True})
    assert r.status_code == 200
    events = _events(r)
    types = [e["type"] for e in events]
    assert "done" in types
    done = next(e for e in events if e["type"] == "done")
    assert done["rum"]["enabled"] is True
    assert "enabled successfully" in done["message"]
    assert captured["kwargs"]["activate"] is True


def test_enable_draft_mode_uses_draft_message(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "compiling draft"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{SVC}/rum/enable", json={"activate": False})
    events = _events(r)
    done = next(e for e in events if e["type"] == "done")
    assert "draft configuration compiled" in done["message"]


def test_enable_streams_error_event_on_orchestrator_failure(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling"}
        raise RuntimeError("FOS bucket creation failed")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{SVC}/rum/enable", json={})
    assert r.status_code == 200
    events = _events(r)
    types = [e["type"] for e in events]
    assert "error" in types
    assert "done" not in types
    err = next(e for e in events if e["type"] == "error")
    assert "fos bucket" in err["message"].lower()


def test_enable_swallows_scheduler_reload_failure_and_still_reports_done(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling"}

    def _boom():
        raise RuntimeError("scheduler unavailable")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", _boom)

    r = client.post(f"/api/services/{SVC}/rum/enable", json={})
    assert r.status_code == 200
    events = _events(r)
    # A broken scheduler reload must not prevent the 'done' event from
    # reaching the client — enabling RUM still succeeded.
    assert any(e["type"] == "done" for e in events)


# ── POST /rum/disable ────────────────────────────────────────────────────


def test_disable_400_when_no_token_anywhere(client, with_config):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": ""}
    r = client.post(f"/api/services/{SVC}/rum/disable", json={})
    assert r.status_code == 400


def test_disable_404_when_service_has_no_config_and_no_token_supplied(client, with_config):
    r = client.post(f"/api/services/{SVC}/rum/disable", json={})
    assert r.status_code == 404


def test_disable_happy_path_streams_done_and_reflects_disabled_state(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN", "rum_enabled": True}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "disabling RUM"}
        with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN", "rum_enabled": False}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{SVC}/rum/disable", json={"activate": True})
    assert r.status_code == 200
    events = _events(r)
    done = next(e for e in events if e["type"] == "done")
    assert done["rum"]["enabled"] is False
    assert "disabled successfully" in done["message"]


def test_disable_draft_mode_uses_draft_message(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "compiling draft"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    r = client.post(f"/api/services/{SVC}/rum/disable", json={"activate": False})
    events = _events(r)
    done = next(e for e in events if e["type"] == "done")
    assert "draft configuration compiled" in done["message"]


def test_disable_uses_supplied_token_without_needing_config(client, with_config, monkeypatch):
    """token in the request body means _get_fastly_token() (which requires
    an existing config) is never consulted."""
    captured: dict = {}

    def fake_run_with_events(func, *args, **kwargs):
        captured["args"] = args
        yield {"type": "status", "message": "disabling"}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{SVC}/rum/disable", json={"token": "SUPPLIED-TOKEN"})
    assert r.status_code == 200
    assert "SUPPLIED-TOKEN" in captured["args"]


def test_disable_streams_error_event_on_orchestrator_failure(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "disabling"}
        raise RuntimeError("bucket deletion failed")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)

    r = client.post(f"/api/services/{SVC}/rum/disable", json={})
    events = _events(r)
    err = next(e for e in events if e["type"] == "error")
    assert "bucket deletion" in err["message"].lower()


def test_disable_swallows_scheduler_reload_failure(client, with_config, monkeypatch):
    with_config[SVC] = {"service_id": SVC, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "disabling"}

    def _boom():
        raise RuntimeError("scheduler down")

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", _boom)

    r = client.post(f"/api/services/{SVC}/rum/disable", json={})
    events = _events(r)
    assert any(e["type"] == "done" for e in events)
