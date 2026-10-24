"""Shared fixtures for tests/core/* suites."""

from __future__ import annotations

import gzip
import io
import json
import os
import uuid
from typing import Any

import pytest


@pytest.fixture
def custom_field_env(
    s3_mock: Any, fos_source: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> dict[str, Any]:
    """Lightweight wrapper around the ingest harness for custom-field suites.

    Sets up moto S3 + a local DuckLake data path + cache, plus the
    ``_get_fos_client`` monkeypatch for the ingest module namespace and
    a config loader stub that returns whatever custom fields the test asks for.
    Returns helpers for seeding gzipped log rows and draining the ingest generator.
    """
    unique_id = f"cf{uuid.uuid4().hex[:10]}"
    fos_source["service_id"] = unique_id
    fos_source["name"] = unique_id
    fos_source["duckdb_path"] = str(tmp_path / "e2e.duckdb")
    cache_path = str(tmp_path / "cache")
    os.makedirs(cache_path, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)

    class _CallerHintShim:
        def __init__(self, client: Any) -> None:
            self._client = client

        def get_paginator(self, op: str, caller_hint: str | None = None) -> Any:
            return self._client.get_paginator(op)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._client, name)

    shim = _CallerHintShim(s3_mock)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: shim)

    from backend.core import iceberg as ice

    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    def _set_custom_fields(custom_fields: list[dict[str, Any]]) -> None:
        cfg = {
            "service_id": fos_source["service_id"],
            "log_fields": {
                "schema_version": 2,
                "groups": ["A"],
                "custom_fields": custom_fields,
            },
        }
        monkeypatch.setattr("backend.config.load_config", lambda _sid: cfg)

    def _seed_gz(key: str, rows: list[dict[str, Any]]) -> None:
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    def _drain_ingest() -> list[Any]:
        from backend.core import ingest as ing

        return list(ing.ingest(source=fos_source))

    return {
        "fos_source": fos_source,
        "cache": cache_path,
        "set_custom_fields": _set_custom_fields,
        "seed_gz": _seed_gz,
        "drain_ingest": _drain_ingest,
    }
