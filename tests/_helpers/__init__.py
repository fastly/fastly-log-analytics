"""Shared test helpers (importable as ``tests._helpers.*``)."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import random
import sys
import tempfile
import time
import types
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.encoders import jsonable_encoder

import backend.core.duckdb_pool as duckdb_pool
from backend.core.log_fields import LOG_FIELD_CATALOG, resolve_enabled_fields
from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME, IP_SPREAD_DIR_NAME
from backend.repositories._base import _safe_table

# ── Sources & TinyFos (from sources.py) ──────────────────────────────────────

_CLOUD_DEFAULTS: dict[str, Any] = {
    "service_name": "Test",
    "endpoint": "us-east-1.object.fastlystorage.app",
    "access_key_id": "key",
    "secret_access_key": "secret",
    "bucket": "bucket",
    "prefix": "",
    "region": "us-east-1",
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
            patch("backend.core.storage._core._ducklake_attach", side_effect=self.attach),
            patch("backend.core.ingest._configure_fos"),
            patch("backend.core.duckdb._configure_fos"),
            patch("backend.core.duckdb.get_memory_connection", side_effect=lambda _src: duckdb.connect()),
            patch("backend.config.HOT_S3_ENDPOINT", False),
        ]
        if self.table_name is not None:
            patches.append(patch("backend.core.storage._core.ducklake_table_name", return_value=self.table_name))
        else:
            patches.append(
                patch(
                    "backend.core.storage._core.ducklake_table_name",
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


# ── Config store fakes & Fastly provisioning mocks (from config_fakes.py) ────


def fake_mutate_config(
    base: dict[str, Any] | Callable[[], dict[str, Any] | None],
    saved: dict[str, Any] | None = None,
    saves: list[dict[str, Any]] | None = None,
    stateful: bool = False,
) -> Callable[..., dict[str, Any] | None]:
    """Build a ``mutate_config`` replacement over an in-memory ``base`` config."""
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
    """Adapt a ``save_config``-style recorder ``save(service_id, cfg)`` into a ``mutate_config`` fake."""

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
    from backend.cron.jobs import metric_snapshots

    batch = metric_snapshots.get_batch(since=since)
    parts = [metric]
    if service_id:
        parts.append(service_id)
    if task:
        parts.append(task)
    return batch.get("|".join(parts), [])


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


# ── DuckDB analytical table fixtures (from duckdb_fixtures.py) ───────────────

DEFAULT_TABLE_NAME = "test_service"


def generate_mock_records(
    config: dict,
    num_logs: int = 100,
    hours_ago: int = 1,
    rng: random.Random | Any = random,
) -> list[dict]:
    """Generate synthetic per-record log entries based on the log_fields schema."""
    enabled_fields = resolve_enabled_fields(config.get("log_fields"))
    logs = []

    now = datetime.now(UTC)
    start_time = now - timedelta(hours=hours_ago)

    ips = ["192.168.1.1", "10.0.0.1", "203.0.113.5", "198.51.100.10"]
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        "curl/7.68.0",
        "python-requests/2.25.1",
    ]
    urls = ["/", "/api/data", "/images/logo.png", "/about", "/login"]

    for _ in range(num_logs):
        log_entry = {}
        log_time = start_time + timedelta(seconds=rng.randint(0, hours_ago * 3600))

        if "timestamp" in enabled_fields:
            log_entry["timestamp"] = log_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            if log_entry["timestamp"].endswith("0000"):
                log_entry["timestamp"] = log_entry["timestamp"][:-4] + "00:00"
        if "ip" in enabled_fields:
            log_entry["ip"] = rng.choice(ips)
        if "status" in enabled_fields:
            log_entry["status"] = rng.choices([200, 404, 500], weights=[0.8, 0.1, 0.1])[0]
        if "elapsed" in enabled_fields:
            log_entry["elapsed"] = rng.randint(1000, 500000)
        if "cache" in enabled_fields:
            log_entry["cache"] = rng.choices(["HIT", "MISS", "PASS"], weights=[0.6, 0.3, 0.1])[0]
        if "resp_bytes" in enabled_fields:
            log_entry["resp_bytes"] = rng.randint(500, 50000)
        if "url" in enabled_fields:
            log_entry["url"] = rng.choice(urls)
        if "ua" in enabled_fields:
            log_entry["ua"] = rng.choice(uas)
        if "method" in enabled_fields:
            log_entry["method"] = "GET"
        if "pop" in enabled_fields:
            log_entry["pop"] = rng.choice(["JFK", "LHR", "SYD"])
        if "ttfb" in enabled_fields:
            ttfb_ms = log_entry.get("elapsed", 20000) * 0.8
            log_entry["ttfb"] = f"{ttfb_ms / 1000000:.6f}"
        if "country" in enabled_fields:
            log_entry["country"] = rng.choice(["US", "GB", "AU", "DE"])
        if "asn" in enabled_fields:
            log_entry["asn"] = rng.choice([7922, 3320, 15169])

        logs.append(log_entry)

    return logs


def generate_mock_logs(config: dict, num_logs: int = 100, hours_ago: int = 1) -> list[dict]:
    """Generate synthetic log entries based on the log_fields schema."""
    return generate_mock_records(config, num_logs=num_logs, hours_ago=hours_ago)


def insert_mock_logs(con, table_name: str, logs: list[dict]):
    """Inserts a list of mock JSON logs directly into the DuckDB connection."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")
        temp_path = f.name

    try:
        raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
        columns_map = {f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"]) for f in raw_fields}
        for log in logs:
            for k, v in log.items():
                if k not in columns_map:
                    if isinstance(v, bool):
                        columns_map[k] = "BOOLEAN"
                    elif isinstance(v, int):
                        columns_map[k] = "BIGINT"
                    elif isinstance(v, float):
                        columns_map[k] = "DOUBLE"
                    else:
                        columns_map[k] = "VARCHAR"

        schema_def = ", ".join([f'"{col}" {dtype}' for col, dtype in columns_map.items()])
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")
        for col, dtype in columns_map.items():
            try:
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" {dtype}')
            except Exception:
                pass

        cols_sql = "{" + ", ".join(f"'{k}': '{v}'" for k, v in columns_map.items()) + "}"
        con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM read_json_auto('{temp_path}', columns={cols_sql})")
    finally:
        os.unlink(temp_path)


def make_seeded_logs_con(
    table_name: str = DEFAULT_TABLE_NAME,
    num_rows: int = 20,
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with the full 43-column analytical schema
    and ``num_rows`` deterministic rows covering status codes, POPs, countries, and latencies.
    """
    con = duckdb.connect(":memory:")
    table = _safe_table(table_name)
    con.execute(
        f"""
        CREATE TABLE {table} (
            "timestamp" TIMESTAMPTZ, "dt" VARCHAR, "timestamp_hour" VARCHAR, "status" INTEGER,
            "country" VARCHAR, "url" VARCHAR, "ip" VARCHAR, "method" VARCHAR, "ua" VARCHAR,
            "pop" VARCHAR, "asn" INTEGER, "city" VARCHAR, "region" VARCHAR, "elapsed" INTEGER,
            "cache" VARCHAR, "ottfb" DOUBLE, "ttfb" DOUBLE, "ottlb" DOUBLE, "waf_sig" VARCHAR,
            "edge" BOOLEAN, "lat" DOUBLE, "lon" DOUBLE, "resp_bytes" INTEGER, "oip" VARCHAR,
            "ost" INTEGER, "tcp_rtt" INTEGER, "ploss" DOUBLE, "rtt_var" INTEGER, "rtt_min" INTEGER,
            "metro" VARCHAR, "ja3" VARCHAR, "ja4" VARCHAR, "ttl" INTEGER, "p_type" VARCHAR,
            "transport" VARCHAR, "c_speed" VARCHAR, "c_type" VARCHAR, "server_region" VARCHAR,
            "retrans" INTEGER, "bw" INTEGER, "delivery_rate" INTEGER, "rid" VARCHAR, "prid" VARCHAR
        )
        """
    )
    base = datetime.now(UTC) - timedelta(hours=1)
    statuses = [200, 200, 200, 200, 404, 500, 200, 304, 301, 200]
    pops = ["LAX", "JFK", "LHR"]
    step_minutes = max(1, 60 // max(num_rows, 1))
    for i in range(num_rows):
        ts = base + timedelta(minutes=i * step_minutes)
        status = statuses[i % len(statuses)]
        con.execute(
            f"INSERT INTO {table} VALUES " + "(" + ", ".join(["?"] * 43) + ")",
            [
                ts,
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%Y-%m-%d-%H"),
                status,
                "US" if i % 3 == 0 else "GB",
                f"/path-{i % 10}",
                f"10.0.0.{i}",
                "GET",
                "Mozilla/5.0",
                pops[i % len(pops)],
                15169 + (i % 5),
                "San Francisco",
                "CA",
                50 + i * 10,
                "HIT" if i % 2 == 0 else "MISS",
                50000.0 + i,
                0.05 + i * 0.01,
                0.1 + i * 0.01,
                "",
                True,
                37.7749,
                -122.4194,
                500 + i * 100,
                "origin-1.example.com",
                status,
                10000 + i * 100,
                0.0,
                100,
                50,
                "807",
                "ja3_fp",
                "ja4_fp",
                300,
                "U",
                "h2",
                "C",
                "broadband",
                "us-west",
                0,
                100000,
                100000,
                f"rid_{i}",
                f"prid_{i % 5}",
            ],
        )
    return con


# ── Rollup writer/reader test helpers (from rollups.py) ──────────────────────

ALL_FIELDS = "all_fields.parquet"
DEFAULT_ANCHOR = datetime(2026, 3, 15, 10, 0, 0, tzinfo=UTC)


@contextmanager
def noop_lock(*_a: Any, **_k: Any) -> Iterator[None]:
    """No-op stand-in for per-service context-manager locks in unit tests."""
    yield


def days_ago_iso(days: int) -> str:
    return (datetime.now(UTC).date() - timedelta(days=days)).strftime("%Y-%m-%d")


def yesterday_iso() -> str:
    return days_ago_iso(1)


def two_days_ago_iso() -> str:
    return days_ago_iso(2)


def three_days_ago_iso() -> str:
    return days_ago_iso(3)


def past_hour(hours_ago: int) -> tuple[str, datetime]:
    """``(partition key "YYYY-MM-DD-HH", hour-aligned datetime)`` for ``hours_ago`` hours back."""
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def write_hour_all_fields(cache_root: str, hour: str) -> None:
    """Write a sentinel ``all_fields.parquet`` so the backfill walk treats ``hour`` as touched."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "field": pa.array(["x"]),
                "value": pa.array(["y"]),
                "count": pa.array([1], type=pa.int64()),
            }
        ),
        os.path.join(d, "all_fields.parquet"),
    )


def build_writer_patches(
    cache_root: Any,
    table: str,
    con_factory: Callable[[], Any],
    *,
    safe_table_target: str = "backend.core.rollups._common._safe_table_for",
) -> tuple[Any, ...]:
    """Return the common 5-patch stack for rollup writer tests."""
    return (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch(safe_table_target, return_value=table),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: con_factory()),
        patch("backend.core.storage._core._get_service_lock", noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    )


@contextmanager
def writer_patches(
    cache_root: Any,
    table: str,
    con: Any,
    *,
    safe_table_target: str = "backend.core.rollups._common._safe_table_for",
) -> Iterator[None]:
    """Context manager entering the 5-patch rollup writer stack."""
    with ExitStack() as stack:
        for p in build_writer_patches(cache_root, table, lambda: con, safe_table_target=safe_table_target):
            stack.enter_context(p)
        yield


def write_hour_bundle(hour_dir: str, filename: str, select_sql_values: str, schema_cols: str) -> None:
    """Write a Parquet bundle file from a SQL ``VALUES`` clause."""
    os.makedirs(hour_dir, exist_ok=True)
    wcon = duckdb.connect()
    try:
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {select_sql_values}) AS t({schema_cols})) "
            f"TO '{hour_dir}/{filename}' (FORMAT PARQUET)"
        )
    finally:
        wcon.close()


def make_stub_runner(
    src: dict[str, Any],
    captured_sql: list[str],
    stub_rows: list[tuple[Any, ...]],
    *,
    table: str = "logs_svc_reader",
) -> Any:
    """Build a ``QueryRunner`` with ``execute()`` stubbed to capture SQL and return canned rows."""
    from backend.repositories._base import QueryRunner

    class _Result:
        def fetchall(self) -> list[tuple[Any, ...]]:
            return stub_rows

        def fetchone(self) -> tuple[Any, ...] | None:
            return stub_rows[0] if stub_rows else None

    class _Conn:
        def execute(self, sql: str, params: Any = None) -> _Result:
            captured_sql.append(sql)
            return _Result()

    runner = QueryRunner.__new__(QueryRunner)
    runner.src = src
    runner._table = table
    runner.execute = _Conn().execute  # type: ignore[method-assign]
    return runner


@dataclass
class RollupGoldenLayout:
    """Unified on-disk rollup directory layout helper for golden characterization suites."""

    root: Path
    anchor: datetime = DEFAULT_ANCHOR
    fns: tuple[str, ...] = ()
    hour_root: Path = field(init=False)
    day_root: Path = field(init=False)
    field_root: Path = field(init=False)
    hour: Path = field(init=False)
    day: Path = field(init=False)
    hour_b: Path = field(init=False)
    day_b: Path = field(init=False)
    bundled: Path = field(init=False)
    perfield: Path = field(init=False)

    def __post_init__(self) -> None:
        r = self.root / "rollups"
        self.hour_root = r / "hour_bundled"
        self.day_root = r / "day_bundled"
        self.field_root = r / "hour"
        self.hour = self.field_root
        self.day = r / "day"
        self.hour_b = self.hour_root
        self.day_b = self.day_root
        self.bundled = self.hour_root
        self.perfield = r / IP_SPREAD_DIR_NAME

    def hkey(self, k: int) -> str:
        return (self.anchor + timedelta(hours=k)).strftime("%Y-%m-%d-%H")

    def hk(self, k: int) -> str:
        return self.hkey(k)

    def dkey(self, k: int) -> str:
        return (self.anchor + timedelta(days=k)).strftime("%Y-%m-%d")

    def dk(self, k: int) -> str:
        return (self.anchor.replace(hour=0) + timedelta(days=k)).strftime("%Y-%m-%d")

    @staticmethod
    def _touch(p: Path) -> None:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")

    def mkroot(self) -> None:
        self.hour_root.mkdir(parents=True, exist_ok=True)
        self.day_root.mkdir(parents=True, exist_ok=True)
        self.field_root.mkdir(parents=True, exist_ok=True)

    def hour_files(self, ks: range | list[int], fns: tuple[str, ...] | None = None) -> None:
        for k in ks:
            d = self.hour_root / f"hour={self.hkey(k)}"
            d.mkdir(parents=True, exist_ok=True)
            for fn in fns or self.fns:
                (d / fn).write_bytes(b"x")

    def day_files(self, ks: range | list[int], variant: str = "std") -> None:
        for k in ks:
            day = self.dkey(k)
            for fn in self.fns:
                if variant == "std" or fn != ALL_FIELDS:
                    d = self.day_root / f"day={day}"
                    d.mkdir(parents=True, exist_ok=True)
                    (d / fn).write_bytes(b"x")
                elif variant == "nested_alt":
                    d = self.day_root / f"day={day}"
                    d.mkdir(parents=True, exist_ok=True)
                    (d / f"day_bundle_{day}.parquet").write_bytes(b"x")
                elif variant == "flat_alt":
                    self.day_root.mkdir(parents=True, exist_ok=True)
                    (self.day_root / f"day_bundle_{day}.parquet").write_bytes(b"x")

    def marker(self, k: int) -> None:
        (self.field_root / "field=requests" / f"hour={self.hkey(k)}").mkdir(parents=True, exist_ok=True)

    def sentinel(self, k: int) -> None:
        d = self.hour_root / f"hour={self.hkey(k)}"
        d.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())])
        pq.write_table(pa.table({"field": [], "value": [], "count": []}, schema=schema), d / ALL_FIELDS)

    def nonempty_allfields(self, k: int) -> None:
        d = self.hour_root / f"hour={self.hkey(k)}"
        d.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())])
        pq.write_table(pa.table({"field": ["country"], "value": ["US"], "count": [1]}, schema=schema), d / ALL_FIELDS)

    def field_hours(self, fields: list[str], ks: range | list[int], fname: str = "part-0.parquet") -> None:
        for f in fields:
            for k in ks:
                self._touch(self.hour / f"field={f}" / f"hour={self.hk(k)}" / fname)

    def field_hour_dirs(self, fields: list[str], ks: list[int]) -> None:
        for f in fields:
            for k in ks:
                (self.hour / f"field={f}" / f"hour={self.hk(k)}").mkdir(parents=True, exist_ok=True)

    def field_days(self, fields: list[str], ks: list[int], fname: str = "part-0.parquet") -> None:
        for f in fields:
            for k in ks:
                self._touch(self.day / f"field={f}" / f"day={self.dk(k)}" / fname)

    def empty_day_dirs(self, fields: list[str], ks: list[int]) -> None:
        for f in fields:
            for k in ks:
                (self.day / f"field={f}" / f"day={self.dk(k)}").mkdir(parents=True, exist_ok=True)

    def bundled_hours(self, ks: range | list[int]) -> None:
        for k in ks:
            self._touch(self.hour_b / f"hour={self.hk(k)}" / ALL_FIELDS)

    def bundled_days(self, ks: list[int], variant: str = "std") -> None:
        for k in ks:
            day = self.dk(k)
            if variant == "std":
                self._touch(self.day_b / f"day={day}" / ALL_FIELDS)
            elif variant == "nested_alt":
                self._touch(self.day_b / f"day={day}" / f"day_bundle_{day}.parquet")
            elif variant == "flat_alt":
                self._touch(self.day_b / f"day_bundle_{day}.parquet")

    def bundles(self, ks: range | list[int], fname: str = IP_SPREAD_BUNDLE_FILENAME) -> None:
        for k in ks:
            self._touch(self.bundled / f"hour={self.hk(k)}" / fname)

    def perfield_hours(self, fields: list[str], ks: range | list[int], fname: str = "part-0.parquet") -> None:
        for f in fields:
            for k in ks:
                self._touch(self.perfield / f"field={f}" / f"hour={self.hk(k)}" / fname)


def build_rollup_golden_layout(
    root: Path,
    builder: Callable[[RollupGoldenLayout], None] | None = None,
    *,
    anchor: datetime = DEFAULT_ANCHOR,
    fns: tuple[str, ...] = (),
) -> RollupGoldenLayout:
    """Create a ``RollupGoldenLayout`` and optionally apply ``builder`` to populate it."""
    lay = RollupGoldenLayout(root=root, anchor=anchor, fns=fns)
    if builder is not None:
        builder(lay)
    return lay


# ── RUM DuckDB test fixtures and beacon insertion helpers (from rum.py) ──────


def _is_faro_payload(beacon: dict[str, Any]) -> bool:
    """Return True if *beacon* uses the nested Grafana Faro wire schema."""
    if "measurements" in beacon or "events" in beacon:
        return True
    meta = beacon.get("meta")
    if isinstance(meta, dict) and (isinstance(meta.get("browser"), dict) or isinstance(meta.get("page"), dict)):
        return True
    return False


@pytest.fixture
def setup_temp_rum_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Set up a temporary DuckDB database for RUM and mock the source resolver."""
    base_db_path = tmp_path / "test.duckdb"
    rum_db_path = tmp_path / "test.rum.duckdb"

    con = duckdb.connect(str(rum_db_path))
    try:
        con.execute(
            """
            CREATE TABLE client_vitals (
                timestamp TIMESTAMPTZ,
                metric_name VARCHAR,
                metric_value DOUBLE,
                metric_rating VARCHAR,
                pathname VARCHAR,
                browser VARCHAR,
                os VARCHAR,
                device VARCHAR,
                cid VARCHAR,
                req_id VARCHAR,
                city VARCHAR DEFAULT NULL,
                region VARCHAR DEFAULT NULL,
                country VARCHAR DEFAULT NULL,
                pop VARCHAR DEFAULT NULL,
                tls VARCHAR DEFAULT NULL,
                ttfb DOUBLE DEFAULT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE client_errors (
                timestamp TIMESTAMPTZ,
                error_message VARCHAR,
                error_file VARCHAR,
                error_line INTEGER,
                error_col INTEGER,
                pathname VARCHAR,
                browser VARCHAR,
                os VARCHAR,
                device VARCHAR,
                cid VARCHAR,
                req_id VARCHAR,
                city VARCHAR DEFAULT NULL,
                region VARCHAR DEFAULT NULL,
                country VARCHAR DEFAULT NULL,
                pop VARCHAR DEFAULT NULL,
                tls VARCHAR DEFAULT NULL,
                ttfb DOUBLE DEFAULT NULL
            )
            """
        )
    finally:
        con.close()

    unique_id = f"test_service_{uuid.uuid4().hex[:8]}"
    mock_source = {
        "name": unique_id,
        "service_id": unique_id,
        "duckdb_path": str(base_db_path),
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }

    duckdb_pool.reset_pool_for_service("test_service")
    duckdb_pool.reset_pool_for_service("test_service_rum")
    monkeypatch.setattr("backend.core.request_context._resolve_source", lambda service_id, read_only=False: mock_source)
    monkeypatch.setattr(
        "backend.core.request_context._resolve_source_or_400", lambda service_id, read_only=False: mock_source
    )
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda service_id: mock_source)

    return rum_db_path


def _rate_vital(metric_name: str | None, val: float) -> str:
    if metric_name == "load_time":
        return "good" if val <= 2.0 else "poor"
    if metric_name == "lcp":
        return "good" if val <= 2.5 else ("needs_improvement" if val <= 4.0 else "poor")
    if metric_name == "cls":
        return "good" if val <= 0.1 else ("needs_improvement" if val <= 0.25 else "poor")
    if metric_name == "inp":
        return "good" if val <= 200 else ("needs_improvement" if val <= 500 else "poor")
    return "good"


def insert_rum_beacons(temp_rum_db_path: Path | str, beacons_list: list[dict[str, Any]]) -> None:
    """Insert synthetic flat beacons or Faro-shaped payloads into ``temp_rum_db_path``."""
    from backend.core.rum_ingest import extract_metrics_from_faro_payload

    duckdb_pool.shutdown_all()

    vitals_rows: list[tuple[Any, ...]] = []
    errors_rows: list[tuple[Any, ...]] = []

    for i, b in enumerate(beacons_list):
        if _is_faro_payload(b):
            received_at_dt = datetime.now(UTC) - timedelta(minutes=i + 5)
            req_id = f"req_{i}_{id(b)}"
            extracted = extract_metrics_from_faro_payload(b, {})
            for item in extracted:
                metric_name = item.get("metric_name")
                if metric_name == "exception":
                    errors_rows.append(
                        (
                            received_at_dt,
                            item.get("error_message"),
                            item.get("error_file"),
                            item.get("error_line"),
                            item.get("error_col"),
                            item.get("pathname"),
                            item.get("browser"),
                            item.get("os"),
                            item.get("device"),
                            item.get("cid"),
                            req_id,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    )
                else:
                    val = item.get("metric_value")
                    if metric_name == "pageLoadTime" and val is not None:
                        try:
                            val = float(val)
                        except Exception:
                            pass
                    rating = item.get("metric_rating")
                    if not rating and val is not None:
                        try:
                            rating = _rate_vital(metric_name, float(val))
                        except Exception:
                            rating = "good"
                    vitals_rows.append(
                        (
                            received_at_dt,
                            metric_name,
                            val,
                            rating,
                            item.get("pathname"),
                            item.get("browser"),
                            item.get("os"),
                            item.get("device"),
                            item.get("cid"),
                            req_id,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    )
            continue

        received_at = b.get("received_at")
        if not received_at:
            received_at = (datetime.now(UTC) - timedelta(minutes=i + 5)).isoformat()
        dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))

        meta = b.get("meta") or {}
        browser_val = meta.get("browser") or "Chrome"
        os_val = meta.get("os") or "macOS"
        device_val = meta.get("device") or "Desktop"
        pathname_val = b.get("pathname") or "/"
        cid_val = b.get("cid") or "test_cid_1"
        req_id_val = b.get("req_id") if "req_id" in b else f"req_{id(b)}"

        city_val = b.get("city") or "Austin"
        region_val = b.get("region") or "TX"
        country_val = b.get("country") or "US"
        pop_val = b.get("pop") or "SIN"
        tls_val = b.get("tls") or "1.3"
        ttfb_val = float(b["ttfb"]) if "ttfb" in b else 45.0
        common_tail = (
            pathname_val,
            browser_val,
            os_val,
            device_val,
            cid_val,
            req_id_val,
            city_val,
            region_val,
            country_val,
            pop_val,
            tls_val,
            ttfb_val,
        )

        for vital_key in ("load_time", "lcp", "cls", "inp"):
            if vital_key in b:
                v_val = float(b[vital_key])
                vitals_rows.append((dt, vital_key, v_val, _rate_vital(vital_key, v_val), *common_tail))

        for exc in b.get("exceptions") or []:
            errors_rows.append(
                (
                    dt,
                    exc.get("value") or exc.get("message") or "TypeError",
                    exc.get("filename") or "main.js",
                    0,
                    0,
                    *common_tail,
                )
            )

    con = duckdb.connect(str(temp_rum_db_path), read_only=False)
    try:
        if vitals_rows:
            con.executemany(
                "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                vitals_rows,
            )
        if errors_rows:
            con.executemany(
                "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                errors_rows,
            )
    finally:
        con.close()


class _CallableHelperModule(types.ModuleType):
    """Module subclass that is also directly callable as ``wire(value) -> str``."""

    def __call__(self, value: Any) -> str:
        return _wire_encode(value)


_mod = sys.modules[__name__]
_mod.__class__ = _CallableHelperModule
for _alias in (
    "tests._helpers.sources",
    "tests._helpers.wire",
    "tests._helpers.ledger",
    "tests._helpers.config_fakes",
    "tests._helpers.duckdb_fixtures",
    "tests._helpers.rollups",
    "tests._helpers.rum",
):
    sys.modules[_alias] = _mod
sources = wire = ledger = config_fakes = duckdb_fixtures = rollups = rum = _mod
