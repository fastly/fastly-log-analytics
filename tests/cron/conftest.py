"""Shared fixtures and helpers for tests/cron/* suites."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest


class _NoopCtx:
    """No-op context manager stub."""

    def __init__(self, value: Any = None) -> None:
        self.value = value

    def __enter__(self) -> Any:
        return self.value

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


class _StubSource(dict):
    """Minimal dict-subclass representing a configured service source."""

    def __init__(self, service_id: str = "svc-1", access_level: str = "read_write", **overrides: Any) -> None:
        base: dict[str, Any] = {
            "name": service_id,
            "service_id": service_id,
            "bucket": "test-fos-bucket",
            "access_level": access_level,
        }
        base.update(overrides)
        super().__init__(base)


class CronRecorder:
    """Captures ``start_cron_run`` and ``log_cron_run`` calls for assertions."""

    def __init__(self, run_id: int = 101) -> None:
        self.run_id = run_id
        self.starts: list[tuple[Any, ...]] = []
        self.logs: list[dict[str, Any]] = []
        self.start_mock = MagicMock(side_effect=self._on_start)
        self.log_mock = MagicMock(side_effect=self._on_log)

    def _on_start(self, *args: Any, **kwargs: Any) -> int:
        self.starts.append((args, kwargs))
        return self.run_id

    def _on_log(self, source: Any, task: str, duration_s: float, status: str, **kwargs: Any) -> None:
        self.logs.append(
            {
                "source": source,
                "task": task,
                "duration_s": duration_s,
                "status": status,
                **kwargs,
            }
        )


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


@pytest.fixture
def fake_source():
    """Factory fixture returning a ``_StubSource`` dict."""

    def _factory(service_id: str = "svc-1", access_level: str = "read_write", **overrides: Any) -> dict[str, Any]:
        return _StubSource(service_id=service_id, access_level=access_level, **overrides)

    return _factory


@pytest.fixture
def cron_Recorder(monkeypatch: pytest.MonkeyPatch) -> CronRecorder:
    """Patch ``start_cron_run`` and ``log_cron_run`` with a :class:`CronRecorder`."""
    rec = CronRecorder()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", rec.start_mock)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", rec.log_mock)
    return rec
