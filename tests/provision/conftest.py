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


_PG_REQUIRED_PROVISION_TESTS = frozenset({"test_reconcile_records_added_and_removed_fields"})


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark pure-unit provisioning tests ``no_postgres`` while preserving PG-backed audit tests."""
    provision_dir = Path(__file__).resolve().parent
    for item in items:
        item_path = Path(str(item.fspath)).resolve()
        if (provision_dir == item_path or provision_dir in item_path.parents) and (
            item.originalname not in _PG_REQUIRED_PROVISION_TESTS and item.name not in _PG_REQUIRED_PROVISION_TESTS
        ):
            item.add_marker(pytest.mark.no_postgres)
