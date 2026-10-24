"""Stand-ins for ``backend.config.mutate_config`` in tests that fake the config store."""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any


def fake_mutate_config(
    base: dict[str, Any] | Callable[[], dict[str, Any] | None],
    saved: dict[str, Any] | None = None,
    saves: list[dict[str, Any]] | None = None,
    stateful: bool = False,
) -> Callable[..., dict[str, Any] | None]:
    """Build a ``mutate_config`` replacement over an in-memory ``base`` config.

    ``fn`` is applied to a deep copy of ``base`` (or of what the ``base`` callable returns; ``None``
    means the service has no config). The result is merged into ``saved`` and appended to ``saves``
    so a test can assert on the final state or on the number of writes. With ``stateful=True`` each
    mutation builds on the previous one (like the real store) instead of starting from ``base`` again.
    """
    held: dict[str, Any] = {}

    def _mutate(service_id: str, fn: Callable[[dict], None], **_kwargs: Any) -> dict[str, Any] | None:
        current = held["cfg"] if stateful and "cfg" in held else (base() if callable(base) else base)
        if current is None:
            return None
        cfg = copy.deepcopy(current)
        fn(cfg)
        held["cfg"] = copy.deepcopy(cfg)
        if saved is not None:
            saved.update(copy.deepcopy(cfg))
        if saves is not None:
            saves.append(copy.deepcopy(cfg))
        return cfg

    return _mutate


def mutate_via_save(save: Callable[[str, dict], Any]) -> Callable[..., dict[str, Any] | None]:
    """Adapt a ``save_config``-style recorder ``save(service_id, cfg)`` into a ``mutate_config`` fake.

    The config is read through ``backend.config.load_config`` (which the test has patched), a deep copy is
    mutated by ``fn`` and handed to ``save``; ``None`` is returned when the service has no config.
    """

    def _mutate(service_id: str, fn: Callable[[dict], None], **_kwargs: Any) -> dict[str, Any] | None:
        from backend import config

        current = config.load_config(service_id)
        if current is None:
            return None
        cfg = copy.deepcopy(current)
        fn(cfg)
        save(service_id, cfg)
        return cfg

    return _mutate


def config_lock_held(service_id: str = "logsvc1") -> bool:
    """Return True if the in-process config lock for ``service_id`` is currently held."""
    from backend import config as svcconfig

    return bool(getattr(svcconfig._lock_state, "held", {}).get(service_id))


def read_persisted_config(service_id: str = "logsvc1") -> dict[str, Any]:
    """Read the raw persisted JSON config for ``service_id`` directly from ``CONFIGS_DIR``."""
    import json

    from backend import config as svcconfig

    return json.loads(svcconfig.config_path(service_id).read_text())


def fetch_metric_snapshot_history(
    metric: str,
    *,
    since: Any,
    service_id: str | None = None,
    task: str | None = None,
) -> list[dict[str, Any]]:
    """Test helper wrapping ``metric_snapshots.get_batch`` for a single metric."""
    from backend.core import metric_snapshots

    batch = metric_snapshots.get_batch(since=since)
    parts = [metric]
    if service_id:
        parts.append(service_id)
    if task:
        parts.append(task)
    return batch.get("|".join(parts), [])


import pytest


@pytest.fixture
def with_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Patch ``load_config``, ``mutate_config``, and ``get_active_service_id`` against an in-memory dict."""
    container: dict[str, Any] = {}

    def fake_load(svc_id: str) -> dict[str, Any] | None:
        return container.get(svc_id)

    def fake_save(svc_id: str, cfg: dict[str, Any]) -> None:
        container[svc_id] = cfg

    def fake_get_active(fallback_to_first: bool = True) -> str | None:
        return next(iter(container), "TestRumAssetsSvc")

    monkeypatch.setattr("backend.config.load_config", fake_load)
    monkeypatch.setattr("backend.config.mutate_config", mutate_via_save(fake_save))
    monkeypatch.setattr("backend.config.get_active_service_id", fake_get_active)
    return container
