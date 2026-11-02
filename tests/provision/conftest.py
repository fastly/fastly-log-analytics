"""Pytest configuration for provisioning tests — isolates CONFIGS_DIR per test."""

from __future__ import annotations

from pathlib import Path

import pytest

from backend import config as svcconfig


@pytest.fixture(autouse=True)
def _isolate_provision_configs_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect ``backend.config.CONFIGS_DIR`` to ``tmp_path / "configs"`` so provisioning tests never pollute repo ``configs/``."""
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    with svcconfig._config_cache_lock:
        svcconfig._config_cache.clear()
    return cfg_dir
