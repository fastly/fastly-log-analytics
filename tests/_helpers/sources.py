"""One ``make_source`` factory for the service-source dicts tests hand to backend code.

Ten test modules each carried their own ``_make_source`` with a different signature. They
collapse to three shapes, selected with ``kind``:

``"lake"``   a DuckLake-backed local warehouse: ``root`` is the pytest ``tmp_path``; the source
             gets ``cache_<name>/`` (plus ``buffer/`` with ``buffer_dir=True``) and a
             ``<name>.duckdb`` path under it.
``"cache"``  only the keys a cache-dir lookup needs: ``root`` is the cache directory itself
             (or ``None``), stored under ``cache_key`` (``_cache_dir_override`` by default).
             ``mkdirs`` pre-creates sub-directories of ``root``.
``"cloud"``  a FOS/S3-backed source with credentials, endpoint and bucket.

Everything the caller passes as a keyword override wins over the kind's defaults, and mutable
override values are copied so a preset bound once (``functools.partial``) never shares state
between tests.
"""

from __future__ import annotations

import copy
from io import BytesIO
from pathlib import Path
from typing import Any

_CLOUD_DEFAULTS: dict[str, Any] = {
    "service_name": "Test",
    "endpoint": "us-east-1.object.fastlystorage.app",
    "access_key_id": "key",
    "secret_access_key": "secret",
    "bucket": "bucket",
    "prefix": "",
    "region": "us-east-1",
    "access_level": "read_write",
    "storage_mode": "cloud",
}


def _fresh(value: Any) -> Any:
    return copy.deepcopy(value) if isinstance(value, dict | list | set) else value


def make_source(
    root: str | Path | None = None,
    name: str | None = None,
    *,
    kind: str = "cache",
    service_id: str | None = None,
    buffer_dir: bool = False,
    cache_key: str = "_cache_dir_override",
    mkdirs: tuple[str, ...] = (),
    **overrides: Any,
) -> dict[str, Any]:
    if kind == "lake":
        if root is None or name is None:
            raise TypeError("kind='lake' needs root (tmp_path) and name")
        base_dir = Path(root)
        cache = base_dir / f"cache_{name}"
        (cache / "buffer" if buffer_dir else cache).mkdir(parents=True, exist_ok=True)
        src: dict[str, Any] = {
            "name": name,
            "service_id": service_id or name,
            "fos_local_warehouse": True,
            "_cache_dir_override": str(cache),
            "duckdb_path": str(base_dir / f"{name}.duckdb"),
        }
    elif kind == "cache":
        src = {
            "name": name or "test_service",
            "service_id": service_id or (name if name else "test-service-id"),
        }
        if root is not None:
            src[cache_key] = str(root)
            for sub in mkdirs:
                (Path(root) / sub).mkdir(parents=True, exist_ok=True)
    elif kind == "cloud":
        resolved = name or "test"
        src = {"name": resolved, "service_id": service_id or resolved, **_fresh(_CLOUD_DEFAULTS)}
    else:
        raise ValueError(f"unknown source kind {kind!r}")
    src.update({k: _fresh(v) for k, v in overrides.items()})
    return src


class TinyFos:
    """Tiny in-memory Fastly Object Storage seam shared by test runners."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, Any]]] = {}
        self.puts = 0

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, Metadata: dict[str, Any], **_kwargs: Any) -> None:
        self.objects[Key] = (Body, Metadata)
        self.puts += 1

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        payload, metadata = self.objects[Key]
        return {"ContentLength": len(payload), "Metadata": metadata}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        payload, metadata = self.objects[Key]
        return {"ContentLength": len(payload), "Metadata": metadata, "Body": BytesIO(payload)}
