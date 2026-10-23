"""Shared fixtures and helpers for tests/cron/* contract suites."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


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
def cron_source(tmp_path: Path):
    """Factory fixture returning :func:`make_cron_source` bound to ``tmp_path``."""

    def _factory(service_id: str = "svc-cron-contract", **overrides: Any) -> dict[str, Any]:
        return make_cron_source(tmp_path, service_id=service_id, **overrides)

    return _factory
