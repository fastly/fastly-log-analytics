"""End-to-end-ish tests for provisioning create + teardown SSE routes.

Goals (per Milestone B / TESTING_PLAN.md):
- ``GET /api/provision/teardown`` against a known service removes the config
  file and reports a terminal SSE event.
- The 404 path fires when no service config exists.
- ``GET /api/provision/execute`` delegates to ``backend.provision.provision``
  with the params it received.

What's NOT in scope here: end-to-end against a real (or fully mocked)
Fastly REST API. That blocks on Milestone A's deferred 0.2 (pytest-httpx
adoption) — production calls go through stdlib ``urllib.request`` in
``backend.core.fastly.client``, which can't be patched cleanly without
wrapping every call site or porting to ``httpx``. Until then we stub the
orchestrator entry points (``provision`` and ``perform_teardown``) at the
boundary so the route layer is exercised without touching the network.
"""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.main import app


def _parse_sse_events(text: str) -> list[dict]:
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        chunk = chunk.strip()
        if chunk.startswith("data:"):
            try:
                out.append(json.loads(chunk[len("data:") :].strip()))
            except json.JSONDecodeError:
                pass
    return out


@pytest.fixture
def isolated_configs_dir(tmp_path, monkeypatch):
    """Point svcconfig at a tmp dir so create/teardown don't touch real configs/.

    ``CONFIGS_DIR`` is a ``pathlib.Path`` (svcconfig calls ``.mkdir()`` on it),
    not a ``str``.
    """
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    return cfg_dir


# ── Teardown ──────────────────────────────────────────────────────────────────


def test_teardown_no_service_id_returns_404(isolated_configs_dir):
    """Calling teardown with a service_id that has no config → 404."""
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json={"service_id": "nope-not-a-real-svc"})
    assert r.status_code == 404


def test_teardown_emits_done_and_removes_config(isolated_configs_dir, tmp_path, monkeypatch):
    """Happy path: configs/{id}.json exists → teardown route emits a 'done'-style
    terminal event, removes the config, and propagates the perform_teardown
    event stream into the SSE response."""
    sid = "svc-teardown-1"

    # Seed a minimal config so the route picks it up
    cfg = {
        "service_id": sid,
        "logging_service_id": sid,
        "name": "Teardown Test Svc",
        "fos_bucket": "td-test-bucket",
        "fos_region": "us-east-1",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "SECRET",
        "fastly_api_key": "fastly-test-token",
        "cdn_secret": "x",
        "provisioning": {
            "fos_key_id": "fk_1",
            "endpoint_name": "EP",
            "cdn_service_id": "cdn_svc_1",
            "cdn_url": "https://cdn.example/",
        },
    }
    svcconfig.save_config(sid, cfg)
    assert os.path.exists(svcconfig.config_path(sid)), "precondition: config seeded"

    # Stub the orchestrator so no Fastly / S3 / scheduler calls happen
    def fake_perform_teardown(state, token, opts=None):
        yield {"type": "status", "message": "removing logging endpoint"}
        yield {"type": "status", "message": "removing FOS bucket"}
        yield {"type": "done", "message": "teardown complete"}

    with (
        patch("backend.provision.perform_teardown", side_effect=fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.scheduler.get_scheduler"),
        # Security: stub token validation. The auth gate itself is
        # exercised in test_provision_teardown_auth.py.
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-LF"}
                if path == "/tokens/self"
                else {"id": sid, "customer_id": "cust-LF"}
            ),
        ),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "test-tok",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,  # keep so we don't try to delete real DBs
                    "remove_cron": False,
                },
            )

    assert r.status_code == 200
    events = _parse_sse_events(r.text)
    assert events, f"expected SSE events, got: {r.text[:300]!r}"
    types = [e.get("type") for e in events]
    assert "done" in types, f"expected a terminal 'done' event, got types: {types}"

    # The config file should be gone — that's the user-visible side effect
    assert not os.path.exists(svcconfig.config_path(sid)), "config should have been removed"


def test_teardown_remove_cache_clears_service_metadata(isolated_configs_dir):
    """``remove_cache=True`` must also clear the per-service metadata SQLite
    (ingested_files, cron_runs, rollups). It lives in the system data dir —
    OUTSIDE the cache dir the rmtree handles — so without an explicit
    ``metadata.teardown`` a re-provision inherits the dead service's
    ingested_files rollup and the usage-log gap panel shows stale "ours"
    counts."""
    sid = "svc-teardown-meta"
    cfg = {
        "service_id": sid,
        "logging_service_id": sid,
        "name": "Teardown Meta Svc",
        "fos_bucket": "td-meta-bucket",
        "fos_region": "us-east-1",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "SECRET",
        "fastly_api_key": "fastly-test-token",
        "cdn_secret": "x",
        "provisioning": {
            "fos_key_id": "fk",
            "endpoint_name": "EP",
            "cdn_service_id": "cdn",
            "cdn_url": "https://cdn.example/",
        },
    }
    svcconfig.save_config(sid, cfg)

    def fake_perform_teardown(state, token, opts=None):
        yield {"type": "done", "message": "teardown complete"}

    meta_calls: list[str] = []

    with (
        patch("backend.provision.perform_teardown", side_effect=fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.scheduler.get_scheduler"),
        patch("backend.core.metadata.teardown", side_effect=lambda s: meta_calls.append(s)),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "c"}
                if path == "/tokens/self"
                else {"id": sid, "customer_id": "c"}
            ),
        ),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "test-tok",
                    "remove_logging": False,
                    "remove_cdn": False,
                    "remove_bucket": False,
                    "remove_cache": True,  # full local wipe → must include metadata
                    "remove_cron": False,
                },
            )

    assert r.status_code == 200
    assert meta_calls == [sid], f"metadata.teardown({sid!r}) must be called on remove_cache; got {meta_calls}"


def test_teardown_skips_perform_teardown_when_no_logging_service(isolated_configs_dir):
    """If the seeded config has no logging_service_id, the route still 200s
    and removes the config — the orchestrator handles missing fields itself."""
    sid = "svc-teardown-min"
    svcconfig.save_config(sid, {"service_id": sid, "name": "minimal"})

    def fake_perform_teardown(state, token, opts=None):
        yield {"type": "done", "message": "no-op teardown"}

    with (
        patch("backend.provision.perform_teardown", side_effect=fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.scheduler.get_scheduler"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-MIN"}
                if path == "/tokens/self"
                else {"id": sid, "customer_id": "cust-MIN"}
            ),
        ),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={"service_id": sid, "token": "test-tok", "remove_cache": False},
            )
    assert r.status_code == 200
    assert not os.path.exists(svcconfig.config_path(sid))


# ── Create / execute ─────────────────────────────────────────────────────────


def test_execute_delegates_to_provision_with_query_args(isolated_configs_dir):
    """Verify the route forwards the query params it received into the
    cfg dict passed to ``backend.provision.provision``. This is the contract
    test for the create flow until full Fastly mocking lands (Milestone C)."""
    sid = "svc-create-1"
    captured: dict = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "message": "provision complete"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="My Service"),
        patch("backend.provision._sync_crontab"),
        patch("backend.scheduler.get_scheduler"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "test-token",
                    "service_id": sid,
                    "fos_bucket_name": "create-test-bucket",
                    "fos_region": "us-east-1",
                    "endpoint_name": "Test Logger",
                    "edge_only": True,
                    "log_period": "60",
                    "enable_cron_sync": True,
                    "enable_cron_compact": True,
                },
            )

    assert r.status_code == 200, r.text[:500]
    cfg = captured.get("cfg")
    assert cfg is not None, "provision() was never called"
    assert cfg["logging_service_id"] == sid
    assert cfg["fos_bucket_name"] == "create-test-bucket"
    assert cfg["endpoint_name"] == "Test Logger"
    assert cfg["fos_region"] == "us-east-1"

    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "done" in types, f"expected a terminal 'done' event, got: {types}"


def test_execute_propagates_orchestrator_error_event(isolated_configs_dir):
    """When provision() yields an error event, it must reach the SSE consumer."""

    def fake_provision(cfg, _resume_from_state=False):
        yield {"type": "status", "message": "starting"}
        yield {"type": "error", "message": "Fastly API: 503 Service Unavailable"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "tok",
                    "service_id": "svc-create-err",
                    "fos_bucket_name": "create-err-bucket",  # valid: ≥3 chars
                },
            )

    events = _parse_sse_events(r.text)
    err = next((e for e in events if e.get("type") == "error"), None)
    assert err is not None, f"expected an error event, got: {events}"
    assert "503" in err["message"]
