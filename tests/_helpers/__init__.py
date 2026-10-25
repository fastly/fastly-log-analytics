"""Shared test helpers (importable as ``tests._helpers.*``)."""

from __future__ import annotations

import asyncio
import copy
import json
import sys
import time
import types
from collections.abc import Callable, Iterable
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
from fastapi.encoders import jsonable_encoder

# ── Sources & TinyFos (from sources.py) ──────────────────────────────────────

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


# ── Wire comparison, SSE & polling helpers (from wire.py) ────────────────────

_TELEMETRY_KEYS = frozenset(
    {
        "debug_queries",
        "debug_calls",
        "debug_sqlite",
        "debug_postgres",
        "section_timings",
        "is_cached",
        "_debug_queries",
        "_debug_calls",
        "_debug_sqlite",
        "_debug_postgres",
        "_section_timings",
        "_is_cached",
    }
)


def _wire_encode(value: Any) -> str:
    return json.dumps(jsonable_encoder(value), separators=(",", ":"), ensure_ascii=False)


def _wire_key(model_cls: type, key: str) -> str:
    """The serialized key for a producer key (a field name or an alias such as ``_approx``)."""
    fields = model_cls.model_fields
    info = fields.get(key)
    if info is None:
        info = next((f for f in fields.values() if key in (f.alias, f.serialization_alias)), None)
    assert info is not None, f"{model_cls.__name__} does not declare producer key {key!r}"
    return info.serialization_alias or info.alias or key


def assert_sections_wire_identical(model_cls: type, produced: dict[str, Any], *, non_empty: Iterable[str] = ()) -> None:
    """Every non-telemetry key the producer emitted must serialize to the same bytes
    through ``model_cls`` as it does through ``jsonable_encoder`` (the untyped path)."""
    for key in non_empty:
        assert produced.get(key), f"fixture did not exercise {key!r}; the comparison would be vacuous"
    model = model_cls.model_validate({k: v for k, v in produced.items() if k not in _TELEMETRY_KEYS})
    dumped = json.loads(model.model_dump_json(by_alias=True))
    for key, value in produced.items():
        if key in _TELEMETRY_KEYS:
            continue
        out_key = _wire_key(model_cls, key)
        assert out_key in dumped, f"{key!r} dropped from the wire"
        assert _wire_encode(dumped[out_key]) == _wire_encode(value), (
            f"{key!r} wire bytes differ from the producer output"
        )


def assert_rows_wire_identical(row_cls: type, rows: list[dict[str, Any]]) -> None:
    """Row-level variant for models used directly as ``list[Row]`` in a response."""
    assert rows, "no rows to compare"
    dumped = [json.loads(row_cls.model_validate(r).model_dump_json(by_alias=True, exclude_unset=True)) for r in rows]
    assert _wire_encode(dumped) == _wire_encode(rows)


def parse_sse_events(text: str) -> list[dict]:
    """Decode every ``data:`` event in an SSE body into a dict, skipping non-JSON payloads."""
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        chunk = chunk.strip()
        if chunk.startswith("data:"):
            try:
                out.append(json.loads(chunk[len("data:") :].strip()))
            except json.JSONDecodeError:
                pass
    return out


def wait_until(
    check_fn: Callable[[], bool],
    timeout: float = 1.0,
    initial_interval: float = 0.001,
    backoff_factor: float = 2.0,
    max_interval: float = 0.05,
    message: str = "",
) -> None:
    """Poll ``check_fn`` until it returns truthy or ``timeout`` elapses."""
    start = time.perf_counter()
    interval = initial_interval
    while True:
        if check_fn():
            return
        if time.perf_counter() - start >= timeout:
            suffix = f": {message}" if message else ""
            raise AssertionError(f"Timed out after {timeout}s waiting for condition{suffix}")
        time.sleep(interval)
        interval = min(interval * backoff_factor, max_interval)


async def await_until(
    check_fn: Callable[[], bool],
    timeout: float = 1.0,
    initial_interval: float = 0.001,
    backoff_factor: float = 2.0,
    max_interval: float = 0.05,
    message: str = "",
) -> None:
    """Async variant of :func:`wait_until`."""
    start = time.perf_counter()
    interval = initial_interval
    while True:
        if check_fn():
            return
        if time.perf_counter() - start >= timeout:
            suffix = f": {message}" if message else ""
            raise AssertionError(f"Timed out after {timeout}s waiting for condition{suffix}")
        await asyncio.sleep(interval)
        interval = min(interval * backoff_factor, max_interval)


# ── Ingest ledger & DuckLake batch convert helpers (from ledger.py) ──────────


def clear_ledger(service_id: str, *, quarantine: bool = False):
    """Wipe ``ingest_ledger`` (and optionally ``quarantined_files``) rows for
    ``service_id`` and return ``(con, cur)``."""
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    if quarantine:
        cur.execute("DELETE FROM quarantined_files WHERE service_id=?", (service_id,))
    con.commit()
    return con, cur


def seed_discovered(
    con,
    cur,
    service_id: str,
    object_keys: list[str] | str,
    *,
    status: str = "discovered",
) -> None:
    """Insert ``object_keys`` into ``ingest_ledger`` for ``service_id``."""
    now = time.time()
    keys = [object_keys] if isinstance(object_keys, str) else object_keys
    for object_key in keys:
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (service_id, object_key) DO UPDATE SET "
            "status=EXCLUDED.status, attempts=0, last_error=NULL, committed_at=NULL",
            (service_id, object_key, status, now),
        )
    con.commit()


def ledger_row(con, service_id: str, object_key: str) -> dict[str, Any]:
    """Fetch a single ``ingest_ledger`` row as a dict."""
    row = con.execute(
        "SELECT status, claimed_by, claimed_at, committed_at, attempts, last_error, raw_deleted_at "
        "FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row is not None, f"no ledger row for {object_key}"
    return dict(row)


def download_stub(s3_to_local: dict[str, str], requested: list[list[str]] | None = None):
    """Build a ``_download_chunk_to_local`` test stub backed by ``s3_to_local``."""

    def _download(fos_client, s3_paths, tmpdir):
        if requested is not None:
            requested.append(list(s3_paths))
        got = {p: s3_to_local[p] for p in s3_paths if p in s3_to_local}
        return got, {v: k for k, v in got.items()}

    return _download


def attacher(catalog: str, data_path: str, read_only_override: bool = False):
    """Build a file-backed ``ducklake:`` attach callback for tests."""

    def _attach(con_arg, src_arg=None, read_only: bool = False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = ", READ_ONLY" if (read_only or read_only_override) else ""
        for attempt in range(40):
            try:
                con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
                break
            except duckdb.Error as exc:
                if "already exists" in str(exc).lower():
                    break
                if attempt == 39:
                    raise
                time.sleep(0.05)
        return True

    return _attach


def lake_reader(attach_fn, *, read_only: bool = False):
    """Open a fresh DuckDB connection attached to the test DuckLake catalog."""
    con = duckdb.connect()
    attach_fn(con, None, read_only=read_only)
    return con


class DuckLakeLedgerEnv:
    """Reusable context manager for DuckLake + ingest ledger convert tests.

    Patches ``load_config``, ``SERVICES_DATA_DIR``, ``get_source_for_service``,
    ``_get_fos_client``, ``_download_chunk_to_local``, ``_ducklake_attach``,
    ``_configure_fos``, and ``ducklake_table_name``.

    Yields ``self``, which is also directly callable as an ``attach(con, src, read_only=...)``
    function for backwards compatibility with ``with _batch_env(...) as attach:``.
    """

    def __init__(
        self,
        service_id: str,
        tmp_path,
        download=None,
        *,
        s3_to_local: dict[str, str] | None = None,
        fos=None,
        attach=None,
        cfg: dict[str, Any] | None = None,
        bucket: str = "test-bucket",
        prefix: str = "",
        read_only_override: bool = False,
        table_name: str | None = "logs",
    ) -> None:
        self.service_id = service_id
        self.tmp_path = tmp_path
        self.bucket = bucket
        self.prefix = prefix
        self.src = {"service_id": service_id, "name": service_id, "bucket": bucket, "prefix": prefix}
        self.catalog = str(tmp_path / "cat.ducklake")
        self.data_path = str(tmp_path / "lakedata")
        self.real_attach = attacher(self.catalog, self.data_path, read_only_override)
        self.attach = attach if attach is not None else self.real_attach
        self.fos = fos if fos is not None else MagicMock()
        self.cfg = cfg if cfg is not None else {"service_id": service_id}
        if download is not None:
            self.download = download
        else:
            self.download = download_stub(s3_to_local or {})
        self.table_name = table_name
        self._stack = []

    def __call__(self, con_arg, src_arg=None, read_only: bool = False):
        return self.real_attach(con_arg, src_arg, read_only=read_only)

    def reader(self, *, read_only: bool = False):
        return lake_reader(self.real_attach, read_only=read_only)

    def __enter__(self):
        patches = [
            patch("backend.config.load_config", return_value=self.cfg),
            patch("backend.core.duckdb.get_source_for_service", return_value=self.src),
            patch("backend.core.ingest._get_fos_client", return_value=self.fos),
            patch("backend.core.ingest._download_chunk_to_local", side_effect=self.download),
            patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=self.attach),
            patch("backend.core.ingest._configure_fos"),
            patch("backend.core.duckdb._configure_fos"),
            patch("backend.core.duckdb.get_memory_connection", side_effect=lambda _src: duckdb.connect()),
            patch("backend.config.HOT_S3_ENDPOINT", False),
        ]
        if self.table_name is not None:
            patches.append(patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value=self.table_name))
        else:
            patches.append(
                patch(
                    "backend.core.iceberg._ducklake.ducklake_table_name",
                    side_effect=lambda src, table_name="logs": table_name,
                )
            )
        self._stack = patches
        for p in self._stack:
            p.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for p in reversed(self._stack):
            p.stop()
        self._stack = []
        return False


class _CallableHelperModule(types.ModuleType):
    """Module subclass that is also directly callable as ``wire(value) -> str``."""

    def __call__(self, value: Any) -> str:
        return _wire_encode(value)


_mod = sys.modules[__name__]
_mod.__class__ = _CallableHelperModule
for _alias in ("tests._helpers.sources", "tests._helpers.wire", "tests._helpers.ledger"):
    sys.modules[_alias] = _mod
sources = wire = ledger = _mod
