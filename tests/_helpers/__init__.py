"""Shared test helpers (importable as ``tests._helpers.*``)."""

from __future__ import annotations

import asyncio
import copy
import json
import random
import sys
import time
import types
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.encoders import jsonable_encoder

from backend.core.log_fields import resolve_enabled_fields

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


# ── Ingest ledger helpers (from ledger.py) ───────────────────────────────────


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


# ── Synthetic log record generators ──────────────────────────────────────────

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


# ── RUM payload helpers (from rum.py) ────────────────────────────────────────


def _is_faro_payload(beacon: dict[str, Any]) -> bool:
    """Return True if *beacon* uses the nested Grafana Faro wire schema."""
    if "measurements" in beacon or "events" in beacon:
        return True
    meta = beacon.get("meta")
    if isinstance(meta, dict) and (isinstance(meta.get("browser"), dict) or isinstance(meta.get("page"), dict)):
        return True
    return False


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
    "tests._helpers.rum",
):
    sys.modules[_alias] = _mod
sources = wire = ledger = config_fakes = rum = _mod
