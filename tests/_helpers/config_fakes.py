"""Stand-ins for ``backend.config.mutate_config`` in tests that fake the config store."""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock


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


def make_scoring_meta(
    log_svc: str = "TestLogSvc123",
    score_svc: str = "ScoringSvcId",
    keys_store: str = "KEYS_STORE",
    cfg_store: str = "CFG_STORE",
    matrix_store: str = "MATRIX_STORE",
    *,
    with_secret: bool = True,
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "scoring_service_id": score_svc,
        "scoring_service_name": f"Session Scoring Service for {log_svc}",
        "scoring_domain": f"fos-{log_svc.lower()}-session-scorer.edgecompute.app",
        "scoring_keys_store_id": keys_store,
        "scoring_config_store_id": cfg_store,
        "scoring_matrix_store_id": matrix_store,
        "aes_key_hex": "00" * 32,
    }
    if with_secret:
        meta["request_secret"] = "fake_request_secret_for_tests_" + "x" * 32
    return meta


def make_happy_path_fastly_mock(log_svc: str = "TestLogSvc123") -> MagicMock:
    """Return sensible defaults for every Fastly API call ``enable_scoring``
    makes after Stage 1 (``ensure_scoring_service``)."""
    versions = [{"number": 100, "active": True}]
    s3_endpoints = [{"name": "Fastly Object Storage Logs"}]
    snippets: list[dict[str, Any]] = []
    backends: list[dict[str, Any]] = []

    def side_effect(
        method: str, path: str, body: dict[str, Any] | None = None, token: str | None = None, **kwargs: Any
    ) -> Any:
        if (method, path) == ("GET", f"/service/{log_svc}/version"):
            return versions
        if (method, path) == ("PUT", f"/service/{log_svc}/version/100/clone"):
            return {"number": 101}
        if (method, path) == ("PUT", f"/service/{log_svc}/version/101"):
            return {}
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/backend"):
            return backends
        if (method, path) == ("POST", f"/service/{log_svc}/version/101/backend") and body is not None:
            backends.append({"name": body["name"]})
            return {"name": body["name"]}
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/snippet"):
            return snippets
        if (method, path) == ("POST", f"/service/{log_svc}/version/101/snippet") and body is not None:
            snippets.append(
                {
                    "name": body["name"],
                    "type": body.get("type"),
                    "content": body.get("content"),
                    "priority": body.get("priority"),
                }
            )
            return snippets[-1]
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/logging/s3"):
            return s3_endpoints
        if method == "PUT" and "/logging/s3/" in path:
            return {}
        if (method, path) == ("GET", f"/service/{log_svc}/version/101/validate"):
            return {"status": "ok"}
        if (method, path) == ("PUT", f"/service/{log_svc}/version/101/activate"):
            return {}
        return {}

    return MagicMock(side_effect=side_effect)


def make_happy_path_fastly_mock_with_post_secret(
    log_svc: str = "TestLogSvc123",
    keys_store: str = "KEYS_STORE",
    *,
    post_secret_409: bool = False,
) -> MagicMock:
    """Variant that also handles the ``request_secret`` heal POST/PATCH on
    the keys ConfigStore."""
    base_mock = make_happy_path_fastly_mock(log_svc=log_svc)
    base_side_effect = base_mock.side_effect

    def side_effect(
        method: str, path: str, body: dict[str, Any] | None = None, token: str | None = None, **kwargs: Any
    ) -> Any:
        if (method, path) == ("POST", f"/resources/stores/config/{keys_store}/item") and body is not None:
            if post_secret_409:
                raise RuntimeError("409 Conflict — item already exists")
            return {"item_key": body["item_key"]}
        if method == "PATCH" and path == f"/resources/stores/config/{keys_store}/item/request_secret":
            return {"ok": True}
        return base_side_effect(method, path, body, token=token, **kwargs)

    return MagicMock(side_effect=side_effect)
