"""Shared fixtures and helpers for tests/cron/* suites."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

try:
    import backend.cron.scheduler as _sched_mod
    from backend.cron.jobs._common import get_service_display_label as _get_svc_label

    if not hasattr(_sched_mod, "get_service_display_label"):
        _sched_mod.get_service_display_label = _get_svc_label  # type: ignore[attr-defined]
except Exception:
    pass


class _StubSource(dict):
    """Minimal dict-subclass representing a configured service source."""

    def __init__(self, service_id: str = "svc-1", **overrides: Any) -> None:
        base: dict[str, Any] = {
            "name": service_id,
            "service_id": service_id,
            "service_name": service_id,
            "logging_service_id": f"log-{service_id}",
            "bucket": "test-fos-bucket",
            "endpoint": "https://s3.example.com",
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
            "region": "us-east-1",
        }
        base.update(overrides)
        super().__init__(base)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure ``FLA_DEV_NO_CRONS`` is unset across cron suites by default."""
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


@pytest.fixture
def stub_source(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Make ``get_source_for_service`` return a stable dict for any service id."""
    src: dict[str, Any] = {
        "name": "fos-test-svc",
        "service_id": "svc-1",
        "bucket": "fos-test-bkt",
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


def make_cron_source(tmp_path: Path, service_id: str = "svc-expire-contract", **overrides: Any) -> dict[str, Any]:
    """Create a sandboxed service cache directory and return a minimal cron source dict."""
    cache_root = tmp_path / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    base: dict[str, Any] = {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-fos-bucket",
    }
    base.update(overrides)
    return base


def patch_maintenance_config(monkeypatch: pytest.MonkeyPatch, **cron_sync: Any) -> None:
    """Patch ``backend.config.load_config`` with the given ``provisioning.cron_sync`` settings."""
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda _service_id: {"provisioning": {"cron_sync": cron_sync}},
    )


@pytest.fixture
def fake_source():
    """Factory fixture returning a ``_StubSource`` dict."""

    def _factory(service_id: str = "svc-1", **overrides: Any) -> dict[str, Any]:
        return _StubSource(service_id=service_id, **overrides)

    return _factory


@pytest.fixture
def stub_cron_envelope(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    """Stub the standard cron envelope helpers so a cron job's progress and
    ``cron_runs`` side effects can be inspected without real DB writes.
    """
    default_run_id = (
        33 if ("test_commit" in request.node.nodeid.lower() or "testcommitjob" in request.node.nodeid.lower()) else 11
    )
    start = MagicMock(return_value=default_run_id)
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)

    start_progress = MagicMock()
    end_progress = MagicMock()
    cleanup = MagicMock()
    log_event = MagicMock()
    monkeypatch.setattr("backend.cron.scheduler.start_progress", start_progress)
    monkeypatch.setattr("backend.cron.scheduler.end_progress", end_progress)
    monkeypatch.setattr("backend.cron.scheduler.cleanup_progress_and_reap", cleanup)
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", log_event)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda *a, **kw: (True, ""))
    monkeypatch.setattr("backend.cron.scheduler._check_buffer_backlog", lambda *a, **kw: "")
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda *a, **kw: False)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common._refresh_status_after_commit", MagicMock())
    return {
        "start": start,
        "log": log,
        "start_progress": start_progress,
        "end_progress": end_progress,
        "log_event": log_event,
    }


def assert_manual_trigger_standard_mode_rejected(
    endpoint_path: str,
    service_id: str,
    base_source: dict[str, Any],
) -> Any:
    """Assert that a High-Scale-only manual trigger endpoint returns 400 in standard mode."""
    from starlette.testclient import TestClient

    from backend.core.request_context import get_service_id, get_source
    from backend.main import app

    standard_src = {**base_source, "deployment_mode": "standard"}
    app.dependency_overrides[get_source] = lambda: standard_src
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        client = TestClient(app)
        resp = client.post(endpoint_path, headers={"x-fastly-service-id": service_id})
        assert resp.status_code == 400
        detail = resp.json()["detail"]
        msg = detail["message"] if isinstance(detail, dict) else str(detail)
        assert "high-scale" in msg.lower()
        return resp
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def assert_manual_trigger_non_admin_rejected(
    endpoint_path: str,
    service_id: str,
    base_source: dict[str, Any],
) -> Any:
    """Assert that a manual trigger endpoint returns 403 when invoked by a non-admin."""
    from fastapi import HTTPException
    from starlette.testclient import TestClient

    from backend.core.request_context import get_service_id, get_source, require_admin
    from backend.main import app

    def reject_analyst() -> None:
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[get_source] = lambda: base_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    app.dependency_overrides[require_admin] = reject_analyst
    try:
        client = TestClient(app)
        resp = client.post(endpoint_path, headers={"x-fastly-service-id": service_id})
        assert resp.status_code == 403
        return resp
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)
        app.dependency_overrides.pop(require_admin, None)
