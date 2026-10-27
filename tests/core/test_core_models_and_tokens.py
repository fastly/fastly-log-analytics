"""Unit tests for core models, session tokens, CID salts, locks, reset, dir stats, custom fields, and VCL."""

from __future__ import annotations

import ast
import contextlib
import copy
import fcntl
import gzip
import inspect
import io
import json
import logging
import os
import pathlib
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

import backend.core.metadata.ingest_log as quarantine
import backend.core.sql_validator as sql_ident
from backend import config
from backend import config as svcconfig
from backend.config import is_rum_enabled
from backend.core import duckdb as _bk_duckdb
from backend.core import ingest as _bk_ingest
from backend.core import log_fields as lf
from backend.core import reset as reset_mod
from backend.core import storage
from backend.core.duckdb import ensure_cid_salt, get_or_generate_cid_salt
from backend.core.ingest import get_catalog_field_ids, get_ingest_type_hints
from backend.core.log_fields import (
    _BUILTIN_FIELD_NAMES,
    _DUCKDB_RESERVED,
    DEFAULT_OFF_FIELD_IDS,
    PRESETS,
    check_log_line_budget,
    estimate_log_line_bytes,
    generate_log_format,
    get_catalog_for_api,
    get_required_edge_headers,
    resolve_enabled_fields,
    validate_custom_field,
)
from backend.core.metadata.base import get_con
from backend.core.reset import _pause_rum_flags, _restore_rum_flags
from backend.core.storage import _core as _storage_core
from backend.core.storage._core import _get_service_lock
from backend.main import app as _fastapi_app
from backend.models.common import CustomFieldCreate, CustomFieldUpdate, FilterSpec
from backend.provision import generate_capture_vcl
from backend.provision.cmcd_fields import (
    _CMCD_CUSTOM_FIELDS,
    get_cmcd_fields,
)
from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS
from backend.repositories._base import _safe_table
from backend.repositories.utils.filters import build_where_clause
from backend.routers import services
from backend.routers.services import core as services_core_router
from backend.services import service_manager as sm
from backend.utils.bounded_cache import CacheRegistry
from tests.conftest import MOCK_SERVICE_ID


# ── Merged from tests/core/test_common_models.py ──
def test_build_where_clause_basic_and_modes():
    start, end = "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"
    params, sql = build_where_clause(start, end, {}, inline_params=False, partition_pruning=True)
    assert "timestamp >=" in sql and "timestamp <=" in sql
    assert len(params) == 6 and params[0] == start and params[3] == end

    filters = {
        "status": FilterSpec(mode="include", values=[200, 404]),
        "country": FilterSpec(mode="exclude", values=["US"]),
    }
    params, sql = build_where_clause(None, None, filters, inline_params=False)
    assert "(CAST(status AS VARCHAR) IN (?, ?))" in sql and "(country NOT IN (?))" in sql
    assert "200" in params and "404" in params and "US" in params

    params_in, sql_in = build_where_clause(None, None, filters, inline_params=True)
    assert params_in == []
    assert "(CAST(status AS VARCHAR) IN ('200', '404'))" in sql_in and "(country NOT IN ('US'))" in sql_in

    _, sql_url = build_where_clause(None, None, {}, actual_cols=["url"])
    assert "url != '/rum-beacon' AND url NOT LIKE '/rum-beacon' || chr(63) || '%'" in sql_url
    _, sql_ip = build_where_clause(None, None, {}, actual_cols=["ip"])
    assert "url != '/rum-beacon'" not in sql_ip


class TestBuildWhereClauseExtended:
    @pytest.mark.parametrize(
        "filters",
        [
            {},
            {"country": FilterSpec(mode="include", values=[])},
            {"country": FilterSpec(mode="include", values=["", "   "])},
        ],
    )
    def test_passthrough_cases(self, filters):
        params, sql = build_where_clause(None, None, filters)
        assert sql == "1=1" and params == []

    @pytest.mark.parametrize(
        ("key", "mode", "values", "inline", "expected_in", "expected_out"),
        [
            ("filter_country", "include", ["US"], False, ["country"], ["filter_country"]),
            ("xfilter_country", "exclude", ["US"], False, ["country NOT IN"], []),
            ("country_2", "include", ["DE"], False, ["country"], ["country_2"]),
            ("url", "include", ["/api/*"], True, ["ILIKE '/api/%'"], []),
            ("url", "exclude", ["/health*"], True, ["NOT ILIKE '/health%'"], []),
            ("url", "include", ["/api", "/static/*"], True, ["IN ('/api')", "ILIKE '/static/%'"], []),
            ("country", "include", [None], False, ["IS NULL"], []),
            ("country", "exclude", [None], False, ["IS NOT NULL"], []),
            ("country", "include", ["US", None], False, ["IN", "IS NULL"], []),
            ("waf_sig_ind", "include", ["XSS"], False, ["list_contains", "waf_sig"], []),
            ("waf_sig_ind", "exclude", ["SQLi"], False, ["NOT list_contains"], []),
        ],
    )
    def test_filter_expressions(self, key, mode, values, inline, expected_in, expected_out):
        _, sql = build_where_clause(None, None, {key: FilterSpec(mode=mode, values=values)}, inline_params=inline)
        assert all(sub in sql for sub in expected_in) and all(sub not in sql for sub in expected_out)

    def test_empty_string_and_partition_pruning_and_timezone(self):
        params, sql = build_where_clause(None, None, {"country": FilterSpec(mode="include", values=["", "   ", "US"])})
        assert sql.count("?") == len(params) == 1 and "US" in params

        _, sql_no_dt = build_where_clause(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", {}, actual_cols=["timestamp"], partition_pruning=True
        )
        _, sql_dt = build_where_clause(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", {}, actual_cols=["timestamp", "dt"], partition_pruning=True
        )
        _, sql_tz = build_where_clause(
            "2024-06-15T18:30:00+05:30", None, {}, inline_params=True, partition_pruning=True
        )
        assert "dt >=" not in sql_no_dt and "timestamp >=" in sql_no_dt
        assert "dt >=" in sql_dt and "dt <=" in sql_dt and "dt >= '2024-06-15'" in sql_tz


class TestClampToFloatPreservesSubMinuteValues:
    def test_clamp_to_float_and_limit1440(self):
        import pydantic

        from backend.models.common import Limit1440, _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        assert clamp(1.0 / 60.0) == pytest.approx(1.0 / 60.0) and clamp(0.0) == pytest.approx(1.0 / 3600.0)
        assert clamp(99999.0) == 1440.0 and clamp(60.0) == 60.0 and clamp(1.5) == 1.5

        class _M(pydantic.BaseModel):
            v: Limit1440 = 5.0

        m = _M(v=1.0 / 60.0)
        assert isinstance(m.v, float) and 0 < m.v < 1 and abs(m.v - 1.0 / 60.0) < 1e-9


def _run_barrier_threads(n: int, fn):
    barrier = threading.Barrier(n)

    def _worker(idx: int) -> None:
        barrier.wait()
        fn(idx)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ── Merged from tests/core/test_cid_salt.py ──
HEX64 = re.compile(r"[0-9a-f]{64}")


@pytest.fixture
def fake_store(monkeypatch):
    saves: list[str] = []
    real_write = svcconfig._atomic_write_json

    def spy(path, data):
        time.sleep(0.005)
        saves.append(path.stem)
        real_write(path, data)

    monkeypatch.setattr(svcconfig, "_atomic_write_json", spy)

    class Store:
        def put(self, sid, cfg):
            real_write(svcconfig.config_path(sid), cfg)
            svcconfig._config_cache.pop(sid, None)

        def get(self, sid):
            return json.loads(svcconfig.config_path(sid).read_text())

        def exists(self, sid):
            return svcconfig.config_path(sid).exists()

    svcconfig._ensure_dirs()
    return Store(), saves


@pytest.mark.security_regression
def test_missing_config_never_uses_the_public_hardcoded_salt(fake_store):
    store, saves = fake_store
    salt = get_or_generate_cid_salt("svc-none")
    assert "default_test_salt" not in salt and HEX64.fullmatch(salt) and saves == [] and not store.exists("svc-none")


@pytest.mark.security_regression
def test_missing_config_logs_loudly(fake_store, caplog):
    with caplog.at_level(logging.WARNING):
        get_or_generate_cid_salt("svc-none")
    assert "cid_salt_no_config" in caplog.text


@pytest.mark.security_regression
def test_missing_config_salt_is_stable_within_the_process(fake_store):
    assert get_or_generate_cid_salt("svc-a") == get_or_generate_cid_salt("svc-b")


@pytest.mark.security_regression
def test_existing_salt_is_returned_unchanged_and_not_resaved(fake_store, caplog):
    store, saves = fake_store
    store.put("svc", {"rum": {"cid_salt": "abc123"}})
    with caplog.at_level(logging.WARNING):
        assert get_or_generate_cid_salt("svc") == "abc123"
    assert saves == [] and "cid_salt_missing" not in caplog.text


@pytest.mark.security_regression
def test_first_use_generates_once_persists_and_logs(fake_store, caplog):
    store, saves = fake_store
    store.put("svc", {"service_id": "svc", "rum": {}})
    with caplog.at_level(logging.WARNING):
        salt = get_or_generate_cid_salt("svc")
    assert HEX64.fullmatch(salt) and store.get("svc")["rum"]["cid_salt"] == salt and saves == ["svc"]
    assert "cid_salt_missing" in caplog.text and svcconfig.load_config("svc")["rum"]["cid_salt"] == salt


@pytest.mark.security_regression
def test_lazy_generation_does_not_clobber_other_config_fields(fake_store):
    store, _ = fake_store
    store.put("svc", {"service_id": "svc", "name": "keep", "rum": {"enabled": True}})
    get_or_generate_cid_salt("svc")
    cfg = store.get("svc")
    assert cfg["name"] == "keep" and cfg["rum"]["enabled"] is True


@pytest.mark.security_regression
def test_concurrent_first_use_agrees_on_a_single_salt(fake_store):
    store, _ = fake_store
    store.put("svc", {"service_id": "svc", "rum": {}})
    results: list[str] = []
    _run_barrier_threads(8, lambda _: results.append(get_or_generate_cid_salt("svc")))
    assert len(set(results)) == 1 and store.get("svc")["rum"]["cid_salt"] == results[0]


_RACER = """
import sys, time
from pathlib import Path
import backend.config as c, backend.core.duckdb as d
c.CONFIGS_DIR = Path(sys.argv[1])
orig = c._atomic_write_json
c._atomic_write_json = lambda p, data: (time.sleep(0.05), orig(p, data))
go = Path(sys.argv[2])
while not go.exists(): time.sleep(0.001)
print(d.get_or_generate_cid_salt("svc"))
"""


@pytest.mark.security_regression
def test_separate_processes_racing_first_use_agree_on_one_salt(tmp_path):
    cfg_dir = tmp_path / "cfgs"
    cfg_dir.mkdir()
    (cfg_dir / "svc.json").write_text(json.dumps({"service_id": "svc", "rum": {}}))
    go = tmp_path / "go"
    procs = [
        subprocess.Popen(
            [sys.executable, "-I", "-c", _RACER, str(cfg_dir), str(go)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONPATH": os.getcwd()},
        )
        for _ in range(4)
    ]
    time.sleep(4)
    go.write_text("")
    outs = [p.communicate(timeout=60) for p in procs]
    for p, (_, err) in zip(procs, outs, strict=True):
        assert p.returncode == 0, err
    salts = {out.strip().splitlines()[-1] for out, _ in outs}
    assert len(salts) == 1 and json.loads((cfg_dir / "svc.json").read_text())["rum"]["cid_salt"] in salts


@pytest.mark.security_regression
def test_new_config_gets_salt_on_first_save(fake_store):
    store, _ = fake_store
    svcconfig.save_config("fresh", {"service_id": "fresh"})
    assert HEX64.fullmatch(store.get("fresh")["rum"]["cid_salt"])


@pytest.mark.security_regression
def test_save_config_never_rekeys_an_existing_salt(fake_store):
    store, _ = fake_store
    svcconfig.save_config("svc", {"service_id": "svc", "rum": {"enabled": True}})
    original = store.get("svc")["rum"]["cid_salt"]
    for rum_patch in ({"rum": {"enabled": False}}, {"rum": {"cid_salt": "f" * 64}}, {}):
        svcconfig.save_config("svc", {"service_id": "svc", **rum_patch})
        assert store.get("svc")["rum"]["cid_salt"] == original


@pytest.mark.security_regression
def test_mutate_config_serializes_concurrent_partial_edits(fake_store):
    store, _ = fake_store
    svcconfig.save_config("svc", {"service_id": "svc", "counters": {}})
    _run_barrier_threads(
        12, lambda i: svcconfig.mutate_config("svc", lambda c, i=i: c["counters"].__setitem__(str(i), i))
    )
    assert sorted(store.get("svc")["counters"]) == sorted(str(i) for i in range(12))


@pytest.mark.security_regression
def test_mutate_config_missing_service_returns_none(fake_store):
    assert svcconfig.mutate_config("nope", lambda c: None) is None


@pytest.mark.security_regression
def test_ensure_cid_salt_generates_when_absent_and_keeps_existing():
    cfg: dict = {}
    first = ensure_cid_salt(cfg)
    assert HEX64.fullmatch(first) and cfg["cid_salt"] == first and ensure_cid_salt(cfg) == first
    assert ensure_cid_salt({"cid_salt": "keep-me"}) == "keep-me"


@pytest.mark.security_regression
def test_services_router_does_not_mint_its_own_salt():
    src = inspect.getsource(services_core_router)
    minted = [
        n
        for n in ast.walk(ast.parse(src))
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "token_hex"
    ]
    assert minted == [] and "ensure_cid_salt" in src


# ── Merged from tests/core/test_ttl_cache_migration.py ──
@pytest.fixture
def _drain_caches():
    CacheRegistry.clear_all()
    yield
    CacheRegistry.clear_all()


def _describe_con(columns: list[str]) -> MagicMock:
    con = MagicMock()
    con.execute.return_value.fetchall.return_value = [(c,) for c in columns]
    return con


@pytest.mark.usefixtures("_drain_caches")
def test_pii_cols_cached_and_describe_failure_not_cached():
    from backend.repositories import query

    con = _describe_con(["ip", "url", "status"])
    first = query._pii_redact_cols(con, "tbl_pii_a")
    assert first == query._pii_redact_cols(con, "tbl_pii_a") and "ip" in first and "url" not in first
    assert con.execute.call_count == 1
    query._pii_redact_cols(con, "tbl_pii_b")
    assert con.execute.call_count == 2
    CacheRegistry.clear("query._PII_COLS_CACHE")
    query._pii_redact_cols(con, "tbl_pii_a")
    assert con.execute.call_count == 3

    bad_con = MagicMock()
    bad_con.execute.side_effect = duckdb.CatalogException("stale")
    assert query._pii_redact_cols(bad_con, "tbl_pii_c") == frozenset()
    assert "ip" in query._pii_redact_cols(_describe_con(["ip"]), "tbl_pii_c")


@pytest.mark.usefixtures("_drain_caches")
def test_compaction_stats_memoised_drained_and_missing_dir(tmp_path):
    from backend.core import local_compaction

    root = tmp_path / "cache"
    part = root / "data" / "timestamp_hour=2026-10-19-00"
    part.mkdir(parents=True)
    (part / "a.parquet").write_bytes(b"x")
    with patch("backend.core.duckdb._cache_dir", return_value=str(root)):
        first = local_compaction.compaction_stats({"name": "svc"})
        (part / "b.parquet").write_bytes(b"x")
        assert first == local_compaction.compaction_stats({"name": "svc"}) and first["total_files"] == 1
        CacheRegistry.clear("core.local_compaction._COMPACTION_STATS_CACHE")
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 2

    CacheRegistry.clear_all()
    missing = tmp_path / "nothing"
    with patch("backend.core.duckdb._cache_dir", return_value=str(missing)):
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 0
        (missing / "data").mkdir(parents=True)
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 0


@pytest.mark.usefixtures("_drain_caches")
def test_storage_stats_cache_hit_force_and_invalidate():
    import backend.core.metadata.base as rec

    con = MagicMock()
    con.execute.return_value.fetchone.return_value = [3]
    rec._storage_stats_cache.clear()
    with (
        patch.object(rec, "get_con_readonly", return_value=con) as opener,
        patch.object(rec, "_open_usage_log", return_value=None),
    ):
        assert rec.get_metadata_storage_stats("svc-ttl") is rec.get_metadata_storage_stats("svc-ttl")
        assert opener.call_count == 1
        rec.get_metadata_storage_stats("svc-ttl", force=True)
        assert opener.call_count == 2
        rec.invalidate_storage_stats_cache("svc-ttl")
        rec.get_metadata_storage_stats("svc-ttl")
        assert opener.call_count == 3


# ── Merged from tests/core/test_config_lock_hygiene.py ──
SID = "svc-lock"


@pytest.fixture()
def _reset_lock_state(monkeypatch):
    monkeypatch.setattr(svcconfig, "_lock_degraded", False)
    svcconfig._ensure_dirs()
    svcconfig.save_config(SID, {"service_id": SID})
    yield


def _salt() -> str:
    return json.loads(svcconfig.config_path(SID).read_text())["rum"]["cid_salt"]


@contextlib.contextmanager
def _held_flock():
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        yield
    finally:
        fcntl.flock(holder, fcntl.LOCK_UN)
        os.close(holder)


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_flock_oserror_is_loud_once_and_flags_health(monkeypatch, caplog):
    monkeypatch.setattr(svcconfig.fcntl, "flock", MagicMock(side_effect=OSError(30, "Read-only file system")))
    with caplog.at_level(logging.ERROR, logger="backend.config"):
        for _ in range(2):
            with svcconfig.config_lock(SID):
                pass
    errors = [r for r in caplog.records if "config_lock_degraded" in r.getMessage() and r.levelno == logging.ERROR]
    assert len(errors) == 1 and svcconfig.config_lock_degraded() is True


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_health_snapshot_surfaces_config_lock_degraded(monkeypatch):
    from backend.routers.admin.health import health_snapshot

    assert health_snapshot()["config_lock_degraded"] is False
    monkeypatch.setattr(svcconfig, "_lock_degraded", True)
    assert health_snapshot()["config_lock_degraded"] is True


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_lock_wait_is_bounded_and_raises_a_clear_timeout(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    with _held_flock():
        start = time.monotonic()
        with pytest.raises(svcconfig.ConfigLockTimeout, match=SID):
            with svcconfig.config_lock(SID):
                pass
        assert time.monotonic() - start < 3


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_lock_is_acquired_after_the_holder_releases(monkeypatch):
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    threading.Timer(0.2, lambda: (fcntl.flock(holder, fcntl.LOCK_UN), os.close(holder))).start()
    with svcconfig.config_lock(SID):
        pass


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_mutate_callback_saving_the_same_service_does_not_self_deadlock(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 1.0)
    seen: dict = {}

    def _outer(cfg):
        cfg["a"] = 1
        inner = svcconfig.load_config(SID)
        inner["b"] = 2
        svcconfig.save_config(SID, inner)
        svcconfig.mutate_config(SID, lambda c: c.__setitem__("c", 3))
        seen.update(svcconfig.load_config(SID))

    svcconfig.mutate_config(SID, _outer)
    assert seen["b"] == 2 and seen["c"] == 3 and svcconfig.load_config(SID)["a"] == 1


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_reentrancy_is_per_thread(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    result: list[str] = []

    def other():
        try:
            with svcconfig.config_lock(SID):
                result.append("acquired")
        except svcconfig.ConfigLockTimeout:
            result.append("timeout")

    with svcconfig.config_lock(SID):
        t = threading.Thread(target=other)
        t.start()
        t.join()
    assert result == ["timeout"]


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_update_status_goes_through_mutate_config(monkeypatch):
    calls = []
    real = svcconfig.mutate_config
    monkeypatch.setattr(svcconfig, "mutate_config", lambda sid, fn, **kw: (calls.append(sid), real(sid, fn, **kw))[1])
    svcconfig.update_status(SID, {"x": 1})
    assert calls == [SID] and svcconfig.get_status(SID)["x"] == 1


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_update_status_does_not_clobber_a_concurrent_writer():
    with svcconfig.config_lock(SID):
        t = threading.Thread(target=lambda: svcconfig.update_status(SID, {"y": 2}))
        t.start()
        time.sleep(0.05)
        cfg = svcconfig._read_config_fresh(SID)
        cfg["marker"] = "kept"
        svcconfig._save_config_unlocked(SID, cfg)
    t.join()
    final = svcconfig.load_config(SID)
    assert final["marker"] == "kept" and final["status"]["y"] == 2


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_snapshot_config_takes_the_lock(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    with _held_flock():
        with pytest.raises(svcconfig.ConfigLockTimeout):
            svcconfig.snapshot_config(SID)
    assert svcconfig.snapshot_config(SID) is not None


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_transient_read_failure_aborts_the_save_instead_of_minting_a_new_salt(monkeypatch):
    before = _salt()
    with monkeypatch.context() as m:
        m.setattr(svcconfig, "_read_config_fresh", MagicMock(side_effect=OSError(5, "Input/output error")))
        with pytest.raises(svcconfig.CidSaltReadError):
            svcconfig.save_config(SID, {"service_id": SID, "rum": {}})
    assert _salt() == before


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_unreadable_missing_file_still_mints_a_salt_for_a_new_config(monkeypatch):
    svcconfig.save_config("svc-new", {"service_id": "svc-new"})
    assert json.loads(svcconfig.config_path("svc-new").read_text())["rum"]["cid_salt"]


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_save_config_cannot_rotate_the_salt():
    before = _salt()
    svcconfig.save_config(SID, {"service_id": SID, "rum": {"cid_salt": "attacker-chosen-salt-value"}})
    assert _salt() == before


# ── Merged from tests/core/test_reset.py ──


def _save_config(**cron_sync_overrides):
    svcconfig.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "name": "test_service",
            "fos_bucket": "test-bucket",
            "fos_prefix": "",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test-key",
            "fos_secret_access_key": "test-secret",
            "access_level": "read_write",
            "provisioning": {"cron_sync": {"enabled": True, **cron_sync_overrides}},
        },
    )


def _seed_metadata_rows():
    con = get_con(MOCK_SERVICE_ID)
    for sql, params in [
        (
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) VALUES ('a.gz', ?, 10, 100, '2026-01-01')",
            (MOCK_SERVICE_ID,),
        ),
        (
            "INSERT INTO ingested_files_summary (source_name, file_count, total_rows, total_bytes) VALUES (?, 1, 10, 100)",
            (MOCK_SERVICE_ID,),
        ),
        (
            "INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json) VALUES ('buf1', ?, '[]')",
            (MOCK_SERVICE_ID,),
        ),
        (
            "INSERT INTO quarantined_files (file_name, source_name, fos_key, error_key, meta_key) VALUES ('bad.gz', ?, 'raw/bad.gz', 'errors/bad.bad.jsonl', 'errors/bad.meta.json')",
            (MOCK_SERVICE_ID,),
        ),
        ("INSERT INTO sources (name, config, table_name) VALUES (?, '{}', 'logs')", (MOCK_SERVICE_ID,)),
        (
            "INSERT INTO views (id, service_id, name, filters_json) VALUES ('v1', ?, 'My View', '{}')",
            (MOCK_SERVICE_ID,),
        ),
        (
            "INSERT INTO alerts (id, service_id, name, metric, operator, threshold, window_min) VALUES ('al1', ?, 'My Alert', 'error_rate', '>', 5.0, 10.0)",
            (MOCK_SERVICE_ID,),
        ),
        (
            "INSERT INTO scoring_labels (id, service_id, sid, label) VALUES ('sl1', ?, 'session1', 'good')",
            (MOCK_SERVICE_ID,),
        ),
        ("INSERT INTO scoring_audit (service_id, action, actor) VALUES (?, 'relabel', 'ui')", (MOCK_SERVICE_ID,)),
        ("INSERT INTO asn_names (asn, name) VALUES (12345, 'Test ASN')", ()),
    ]:
        con.execute(sql, params)
    con.commit()


def _table_count(table: str) -> int:
    return get_con(MOCK_SERVICE_ID).execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


@pytest.fixture
def _stub_storage_catalog(monkeypatch):
    monkeypatch.setattr("backend.core.storage._core.init_storage_table", MagicMock())
    monkeypatch.setattr("backend.core.storage._core.update_storage_view", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.get_connection", MagicMock())


@pytest.fixture
def fake_scheduler():
    return MagicMock()


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_wipes_operational_preserves_config_and_meta(s3_mock, fos_source, fake_scheduler, monkeypatch):
    from backend.core.storage._core import ducklake_table_name

    monkeypatch.setattr(svcconfig, "DUCKLAKE_DATA_PATH", "s3://test-bucket/ducklake/")
    monkeypatch.setenv("ALLOW_REMOTE_S3_PARQUET_SCAN", "1")
    lake_key = f"ducklake/main/{ducklake_table_name(fos_source, 'logs')}/data/foo.parquet"
    other_tenant_key = "ducklake/main/logs_other_service/data/keep.parquet"
    _save_config()
    _seed_metadata_rows()
    evidence = quarantine.capture_evidence(
        MOCK_SERVICE_ID,
        "request",
        "raw/request/bad.gz",
        b'{"invalid":\x00}\n',
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid JSON",
    )
    evidence_path = quarantine._evidence_path(MOCK_SERVICE_ID, int(evidence["id"]))
    assert evidence_path.exists()
    raw_key = "raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz"
    for k, body in [
        (lake_key, b"x"),
        (other_tenant_key, b"y"),
        ("meta/admin_state.json", b"{}"),
        ("errors/bad.bad.jsonl", b"bad"),
        (raw_key, b"raw"),
    ]:
        s3_mock.put_object(Bucket="test-bucket", Key=k, Body=body)

    events = list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler))
    assert events[-1]["type"] == "done"

    for table in (
        "ingested_files",
        "ingested_files_summary",
        "ingest_in_flight",
        "quarantined_files",
        "quarantine_evidence",
    ):
        assert _table_count(table) == 0, f"{table} should be wiped"
    assert not evidence_path.exists()
    for table in ("sources", "views", "alerts", "scoring_labels", "scoring_audit", "asn_names", "audit_logs"):
        assert _table_count(table) == 1, f"{table} should have 1 row"

    keys = {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}
    assert lake_key not in keys and "errors/bad.bad.jsonl" not in keys
    assert other_tenant_key in keys, "reset must only purge this service's DuckLake table directory"
    assert "meta/admin_state.json" in keys and raw_key in keys
    assert fake_scheduler.call_count >= 2
    assert svcconfig.load_config(MOCK_SERVICE_ID)["provisioning"]["cron_sync"]["enabled"] is True


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_delete_raw_logs_true_purges_raw_prefix(s3_mock, fos_source, fake_scheduler):
    _save_config()
    raw_key = "raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz"
    s3_mock.put_object(Bucket="test-bucket", Key=raw_key, Body=b"raw")
    list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, delete_raw_logs=True, reload_scheduler=fake_scheduler))
    assert raw_key not in {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_cron_busy_blocks_reset(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    monkeypatch.setattr("backend.core.metadata.cron_busy", lambda service_id: True)
    with pytest.raises(RuntimeError, match="Active sync or commit"):
        next(reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler))
    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1)
    lock.release()


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_concurrent_reset_is_excluded_by_service_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    monkeypatch.setattr(reset_mod, "_LOCK_TIMEOUT_S", 0.1)
    lock = _get_service_lock(MOCK_SERVICE_ID)
    held, release = threading.Event(), threading.Event()

    def _hold_lock():
        lock.acquire()
        held.set()
        release.wait(timeout=5)
        lock.release()

    holder = threading.Thread(target=_hold_lock, daemon=True)
    holder.start()
    held.wait(timeout=5)
    try:
        with pytest.raises(RuntimeError, match="Another operation is in progress"):
            next(reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler))
    finally:
        release.set()
        holder.join(timeout=5)


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_error_mid_reset_still_resumes_scheduler_and_releases_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    s3_mock.put_object(Bucket="test-bucket", Key="errors/foo.bad.jsonl", Body=b"x")
    monkeypatch.setattr(
        "backend.core.ingest._delete_objects_robust", MagicMock(side_effect=RuntimeError("FOS unreachable"))
    )
    with pytest.raises(RuntimeError, match="FOS unreachable"):
        list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler))
    assert svcconfig.load_config(MOCK_SERVICE_ID)["provisioning"]["cron_sync"]["enabled"] is True
    assert fake_scheduler.call_count >= 2
    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1)
    lock.release()


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_no_service_raises_value_error(fake_scheduler):
    with pytest.raises(ValueError, match="No service found"):
        next(reset_mod.reset_service_logs("does-not-exist"))


def _drain(gen):
    yielded = []
    try:
        while True:
            yielded.append(next(gen))
    except StopIteration as stop:
        return {"yielded": yielded, "return": stop.value}


@pytest.mark.usefixtures("_stub_storage_catalog")
@pytest.mark.parametrize(
    ("pages", "exclude_prefix", "expected_calls", "expected_return", "expected_msgs"),
    [
        (
            [
                {"Contents": [{"Key": "ducklake/a.parquet"}, {"Key": "ducklake/b.parquet"}]},
                {"Contents": [{"Key": "ducklake/c.parquet"}]},
            ],
            None,
            [["ducklake/a.parquet", "ducklake/b.parquet"], ["ducklake/c.parquet"]],
            3,
            ["Deleted 2 thing(s) so far...", "Deleted 3 thing(s) so far..."],
        ),
        (
            [
                {"Contents": [{"Key": "meta/admin_state.json"}]},
                {"Contents": []},
                {"Contents": [{"Key": "ducklake/data/x.parquet"}]},
            ],
            "meta/",
            [["ducklake/data/x.parquet"]],
            1,
            ["Deleted 1 thing(s) so far..."],
        ),
    ],
)
def test_purge_prefix_pagination_and_exclusion(pages, exclude_prefix, expected_calls, expected_return, expected_msgs):
    fos = MagicMock()
    fos.get_paginator.return_value.paginate.return_value = iter(pages)
    delete_calls = []

    def _fake_delete(fos_client, bucket, keys):
        delete_calls.append(list(keys))
        return len(keys)

    kwargs = {"exclude_prefix": exclude_prefix} if exclude_prefix else {}
    result = _drain(reset_mod._purge_prefix(fos, "bucket", "ducklake/", _fake_delete, label="thing(s)", **kwargs))
    assert result["return"] == expected_return
    assert delete_calls == expected_calls
    assert [e["message"] for e in result["yielded"]] == expected_msgs


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_remove_tree_with_progress_reports_batches_and_removes_everything(tmp_path):
    for i in range(5):
        (tmp_path / f"f{i}.parquet").write_bytes(b"x")
    result = _drain(reset_mod._remove_tree_with_progress(str(tmp_path), batch_size=2))
    assert result["return"] == 5
    assert [e["message"] for e in result["yielded"]] == [
        "Removed 2 local cache files so far...",
        "Removed 4 local cache files so far...",
    ]
    assert not os.path.exists(tmp_path)


@pytest.mark.usefixtures("_stub_storage_catalog")
def test_rum_pause_and_restore_flags():

    cfg = {"rum_enabled": True, "rum": {"faro_version": "1.2.3"}}
    assert is_rum_enabled(cfg)
    paused = _pause_rum_flags(cfg)
    assert paused is not None and not is_rum_enabled(cfg)
    _restore_rum_flags(cfg, paused)
    assert "enabled" not in cfg["rum"] and cfg["rum_enabled"] is True and is_rum_enabled(cfg)

    cfg2 = {"rum_enabled": False, "rum": {"enabled": True}}
    paused2 = _pause_rum_flags(cfg2)
    assert paused2 is not None
    _restore_rum_flags(cfg2, paused2)
    assert cfg2["rum"]["enabled"] is True and cfg2["rum_enabled"] is False

    cfg3: dict = {"rum": {}}
    assert _pause_rum_flags(cfg3) is None and cfg3 == {"rum": {}}


# ── Merged from tests/services/test_service_manager.py ──


def _wait_for(condition_fn, timeout: float = 2.0, interval: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def _reset_dir_stats(monkeypatch):
    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())


@pytest.mark.usefixtures("_reset_dir_stats")
def test_get_dir_stats_missing_and_recursive_counts(tmp_path):
    missing = str(tmp_path / "does-not-exist")
    assert sm._get_dir_stats(missing) == (0, 0)
    assert _wait_for(lambda: missing in sm._dir_stats_cache)

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.parquet").write_bytes(b"y" * 200)
    (sub / "c.parquet").write_bytes(b"z" * 50)
    assert _wait_for(lambda: sm._get_dir_stats(str(tmp_path)) == (350, 3))


@pytest.mark.usefixtures("_reset_dir_stats")
def test_get_dir_stats_returns_cached_value_within_ttl(tmp_path):
    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)
    sm._get_dir_stats(path)
    assert _wait_for(lambda: sm._get_dir_stats(path) == (100, 1))
    (tmp_path / "b.parquet").write_bytes(b"y" * 200)
    assert sm._get_dir_stats(path) == (100, 1)


@pytest.mark.usefixtures("_reset_dir_stats")
def test_get_dir_stats_returns_stale_value_and_refreshes_in_background(tmp_path):
    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 999, 999)
    (tmp_path / "b.parquet").write_bytes(b"y" * 50)

    t0 = time.monotonic()
    size, count = sm._get_dir_stats(path)
    assert (size, count) == (999, 999) and (time.monotonic() - t0) < 0.5
    assert _wait_for(lambda: sm._dir_stats_cache.get(path, (0, 0, 0))[2] == 2, timeout=2.0)
    assert sm._get_dir_stats(path) == (150, 2)


@pytest.mark.usefixtures("_reset_dir_stats")
def test_get_dir_stats_first_ever_call_is_non_blocking(tmp_path):
    (tmp_path / "a.parquet").write_bytes(b"x" * 42)
    path = str(tmp_path)
    t0 = time.monotonic()
    assert sm._get_dir_stats(path) == (0, 0) and (time.monotonic() - t0) < 0.5
    assert _wait_for(lambda: sm._get_dir_stats(path) == (42, 1))


@pytest.mark.usefixtures("_reset_dir_stats")
def test_get_dir_stats_coalesces_concurrent_refreshes(tmp_path, monkeypatch):
    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)
    refresh_count = {"n": 0}
    real_walk = sm._walk_dir_stats

    def _slow_walk(p):
        refresh_count["n"] += 1
        time.sleep(0.05)
        return real_walk(p)

    monkeypatch.setattr(sm, "_walk_dir_stats", _slow_walk)
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 0, 0)

    threads = [threading.Thread(target=lambda: sm._get_dir_stats(path)) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert _wait_for(lambda: len(sm._dir_stats_refresh_in_flight) == 0, timeout=2.0)
    assert refresh_count["n"] == 1


@pytest.mark.usefixtures("_reset_dir_stats")
def test_get_dir_stats_coalesces_cold_first_arrivals(tmp_path, monkeypatch):
    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)
    walk_count = {"n": 0}
    real_walk = sm._walk_dir_stats
    walk_blocker = threading.Event()

    def _slow_walk(p):
        walk_count["n"] += 1
        walk_blocker.wait(timeout=5.0)
        return real_walk(p)

    monkeypatch.setattr(sm, "_walk_dir_stats", _slow_walk)
    results = []
    results_lock = threading.Lock()
    call_barrier = threading.Barrier(11)

    def _call():
        call_barrier.wait(timeout=5.0)
        r = sm._get_dir_stats(path)
        with results_lock:
            results.append(r)

    threads = [threading.Thread(target=_call) for _ in range(10)]
    for t in threads:
        t.start()
    call_barrier.wait(timeout=5.0)
    time.sleep(0.1)
    walk_blocker.set()
    for t in threads:
        t.join()

    assert walk_count["n"] == 1
    assert all(r == (0, 0) for r in results)
    assert _wait_for(lambda: sm._get_dir_stats(path) == (100, 1))


@pytest.mark.usefixtures("_reset_dir_stats")
def test_get_dir_stats_recovers_from_thread_start_failure_and_invariants(tmp_path, monkeypatch):
    path = str(tmp_path / "doesnt-matter")
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 7, 7)

    class _ExplodingThread:
        def __init__(self, *a, **kw):
            pass

        def start(self):
            raise RuntimeError("can't start new thread")

    monkeypatch.setattr(sm.threading, "Thread", _ExplodingThread)
    assert sm._get_dir_stats(path) == (7, 7)
    assert path not in sm._dir_stats_refresh_in_flight
    assert sm._DIR_STATS_TTL_SEC >= 300
    assert not hasattr(sm, "_bust_dir_stats_cache")


# ── Merged from tests/core/test_custom_fields.py ──


@pytest.fixture
def custom_field_env(
    s3_mock: Any, fos_source: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> dict[str, Any]:
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

    if hasattr(storage, "_view_cache"):
        storage._view_cache.clear()

    def _set_custom_fields(custom_fields: list[dict[str, Any]]) -> None:
        cfg = {
            "service_id": fos_source["service_id"],
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields},
        }
        monkeypatch.setattr("backend.config.load_config", lambda _sid: cfg)

    def _seed_gz(key: str, rows: list[dict[str, Any]]) -> None:
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    return {
        "fos_source": fos_source,
        "cache": cache_path,
        "set_custom_fields": _set_custom_fields,
        "seed_gz": _seed_gz,
        "drain_ingest": lambda: list(_bk_ingest.ingest(source=fos_source)),
    }


# ── Custom-field byte budgets & validator ──
_CFG = {
    "groups": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "L", "M"],
    "custom_fields": get_cmcd_fields(True) + [dict(c) for c in _SCORING_CUSTOM_FIELDS],
    "field_overrides": {},
}
_UUID_LEN = 36


def _caps(fmt: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for m in re.finditer(r'"(\w+)":"?%\{[^}]*?substr\([^,]+,\s*0,\s*(\d+)\)', fmt):
        out.setdefault(m.group(1), int(m.group(2)))
    return out


def test_cmcd_sid_fits_a_full_uuid():
    caps = _caps(generate_log_format(_CFG))
    assert "cmcd_sid" in caps and caps["cmcd_sid"] >= _UUID_LEN


def test_cmcd_cid_fits_a_realistic_content_id():
    assert _caps(generate_log_format(_CFG)).get("cmcd_cid", 0) >= _UUID_LEN


def test_no_custom_field_is_clamped_to_zero():
    fmt = generate_log_format(_CFG)
    zeros = [m.group(1) for m in re.finditer(r'"(\w+)":[^,]*?substr\([^,]+,\s*0,\s*0\)', fmt)]
    assert zeros == []


def test_raising_a_byte_limit_raises_the_char_cap():
    for limit, expected_cap in ((60, 10), (600, 100)):
        cfg = copy.deepcopy(_CFG)
        for cf in cfg["custom_fields"]:
            if cf["name"] == "cmcd_sid":
                cf["byte_limit"] = limit
        assert _caps(generate_log_format(cfg))["cmcd_sid"] == expected_cap


def test_widened_budgets_stay_inside_the_line_limit():
    assert check_log_line_budget(_CFG) is None
    assert estimate_log_line_bytes(_CFG) < 16384


def test_catalog_enrichment():
    by_id = {f["id"]: f for f in get_catalog_for_api()}
    assert by_id["resp_bytes"].get("formatter") == "bytes"
    assert by_id["country"].get("formatter") == "country"
    assert by_id["elapsed"].get("unit") == "µs"
    assert by_id["tls"].get("formatter") == "number" and by_id["tls"].get("precision") == 1


def _base_field(**overrides) -> dict:
    return {
        "name": "my_field",
        "label": "My Field",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
        **overrides,
    }


_NAME_REGEX = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_FORBIDDEN_VCL_CHARS = ("\n", ";", "//", "/*", "#")


@given(name=st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=1, max_size=48))
def test_regex_match_plus_clean_name_passes_validator(name: str):
    assume(_NAME_REGEX.match(name) and name not in _DUCKDB_RESERVED and name not in _BUILTIN_FIELD_NAMES)
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert not any("lowercase alphanumeric" in e for e in errors)


@given(name=st.text(min_size=1, max_size=80).filter(lambda s: not _NAME_REGEX.match(s)))
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_regex_mismatch_always_errors(name: str):
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert any("lowercase alphanumeric" in e for e in errors)


@given(expr=st.text(min_size=1, max_size=400).filter(lambda s: s.strip() and any(c in s for c in _FORBIDDEN_VCL_CHARS)))
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_vcl_expression_with_forbidden_char_always_errors(expr: str):
    errors = validate_custom_field(_base_field(vcl_log_expression=expr), existing_names=[])
    assert [e for e in errors if "newlines" in e or "semicolons" in e or "comments" in e]


@given(expr_body=st.text(alphabet=st.characters(blacklist_characters="\n;/#"), min_size=1, max_size=500))
def test_vcl_expression_clean_and_under_limit_passes(expr_body: str):
    assume(expr_body.strip())
    errors = validate_custom_field(_base_field(vcl_log_expression=expr_body), existing_names=[])
    bad = [e for e in errors if any(k in e for k in ("newlines", "semicolons", "comments", "≤ 512", "not be empty"))]
    assert not bad


@given(extra_len=st.integers(min_value=1, max_value=2000))
def test_vcl_expression_over_limit_always_errors(extra_len: int):
    errors = validate_custom_field(_base_field(vcl_log_expression="a" * (512 + extra_len)), existing_names=[])
    assert any("≤ 512" in e for e in errors)


@given(n=st.integers(min_value=-(2**31), max_value=2**31).filter(lambda x: x < 1 or x > 1024))
def test_bytes_estimate_outside_1_1024_always_errors(n: int):
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    assert any("bytes_estimate" in e for e in errors)


@given(n=st.integers(min_value=1, max_value=1024))
def test_bytes_estimate_within_range_passes(n: int):
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    assert not [e for e in errors if "bytes_estimate" in e and not e.startswith("WARN:")]


# ── Cross-service isolation ──
@pytest.fixture
def app():
    a = FastAPI()
    a.include_router(services_core_router.router)
    return a


def _save_service_config(service_id: str, custom_fields: list[dict] | None = None) -> dict:
    cfg = {
        "service_id": service_id,
        "name": service_id,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A"],
            "field_overrides": {},
            "custom_fields": list(custom_fields or []),
        },
    }
    svcconfig.save_config(service_id, cfg)
    return cfg


def _cf(name: str, duckdb_type: str = "VARCHAR", value_type: str = "string") -> dict:
    return {
        "name": name,
        "label": name,
        "description": "",
        "vcl_log_expression": "req.http.X-Test",
        "collection_stage": "edge",
        "duckdb_type": duckdb_type,
        "value_type": value_type,
        "bytes_estimate": 20,
        "nullable": True,
        "enabled": True,
        "show_in_dashboard": False,
        "show_in_logs": True,
        "filterable": True,
    }


@pytest.mark.security_regression
def test_custom_field_in_service_a_not_visible_in_service_b(app):
    svc_a, svc_b = "svc-aaa", "svc-bbb"
    _save_service_config(svc_a)
    _save_service_config(svc_b)

    with TestClient(app) as c:
        r = c.post(f"/api/services/{svc_a}/custom-fields", json=_cf("svc_a_field"))
        assert r.status_code == 200 and r.json()["field"]["name"] == "svc_a_field"

        r_a = c.get(f"/api/services/{svc_a}/custom-fields")
        r_b = c.get(f"/api/services/{svc_b}/custom-fields")
        assert r_a.status_code == 200 and r_b.status_code == 200
        names_a = {f["name"] for f in r_a.json()["fields"]}
        names_b = {f["name"] for f in r_b.json()["fields"]}
        assert "svc_a_field" in names_a and "svc_a_field" not in names_b

        r_a_lf = c.get(f"/api/services/{svc_a}/log-fields")
        r_b_lf = c.get(f"/api/services/{svc_b}/log-fields")
        assert {f["name"] for f in r_a_lf.json()["log_fields"]["custom_fields"]} == names_a
        assert {f["name"] for f in r_b_lf.json()["log_fields"]["custom_fields"]} == names_b

    cfg_a, cfg_b = svcconfig.load_config(svc_a), svcconfig.load_config(svc_b)
    assert cfg_a is not None and cfg_b is not None
    assert "svc_a_field" in {f.name for f in _storage_core.get_arrow_schema(cfg_a["log_fields"])}
    assert "svc_a_field" not in {f.name for f in _storage_core.get_arrow_schema(cfg_b["log_fields"])}
    assert _bk_duckdb._safe_table_name(svc_a) != _bk_duckdb._safe_table_name(svc_b)


@pytest.mark.security_regression
def test_same_field_name_in_both_services_has_independent_values(tmp_path):
    svc_a, svc_b = "svc-A-values", "svc-B-values"
    _save_service_config(svc_a, custom_fields=[_cf("my_field")])
    _save_service_config(svc_b, custom_fields=[_cf("my_field")])

    for svc in (svc_a, svc_b):
        assert "my_field" in {f.name for f in _storage_core.get_arrow_schema(svcconfig.load_config(svc)["log_fields"])}

    pq_a, pq_b = tmp_path / "a" / "rows.parquet", tmp_path / "b" / "rows.parquet"
    pq_a.parent.mkdir()
    pq_b.parent.mkdir()
    pq.write_table(pa.table({"my_field": ["alpha-from-A"]}), str(pq_a))
    pq.write_table(pa.table({"my_field": ["beta-from-B"]}), str(pq_b))

    con = duckdb.connect(":memory:")
    try:
        table_a, table_b = _bk_duckdb._safe_table_name(svc_a), _bk_duckdb._safe_table_name(svc_b)
        assert table_a != table_b
        con.execute(f"CREATE OR REPLACE VIEW {table_a} AS SELECT * FROM read_parquet('{pq_a}')")
        con.execute(f"CREATE OR REPLACE VIEW {table_b} AS SELECT * FROM read_parquet('{pq_b}')")
        rows_a = con.execute(f"SELECT my_field FROM {table_a}").fetchall()
        rows_b = con.execute(f"SELECT my_field FROM {table_b}").fetchall()
    finally:
        con.close()

    assert rows_a == [("alpha-from-A",)] and rows_b == [("beta-from-B",)]


# ── Hard delete & lifecycle helpers ──
def _row(ts: datetime, idx: int, **extra):
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
        **extra,
    }


def _connect(env):
    con = _bk_duckdb.get_connection(env["fos_source"])
    storage.update_storage_view(con, env["fos_source"])
    return con, _safe_table(env["fos_source"]["name"])


def _query_all(env, columns: str) -> list:
    con, view_name = _connect(env)
    try:
        return con.execute(f"SELECT {columns} FROM {view_name} ORDER BY url").fetchall()
    finally:
        con.close()


def _ingest_and_commit(env, expected: int):
    done = next((e for e in env["drain_ingest"]() if e["type"] == "done"), None)
    assert done is not None and done["rows_inserted"] == expected, f"expected {expected} rows, got {done}"
    storage.commit_buffer(env["fos_source"])


def test_hard_delete_preserves_old_data_and_allows_new_ingest(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"]([{"name": "doomed", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"doomed":""'}])
    storage.init_storage_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=10/minute=00/2026-05-22T10-00-00.svc.gz",
        [_row(base, 0, doomed="first"), _row(base + timedelta(seconds=1), 1, doomed="second")],
    )
    _ingest_and_commit(env, expected=2)

    env["set_custom_fields"]([])
    if hasattr(storage, "_view_cache"):
        storage._view_cache.clear()
    storage.init_storage_table(env["fos_source"])

    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=11/minute=00/2026-05-22T11-00-00.svc.gz",
        [_row(base + timedelta(hours=1), 2)],
    )
    _ingest_and_commit(env, expected=1)

    by_url = dict(_query_all(env, "url, doomed"))
    assert by_url == {"/path/0": "first", "/path/1": "second", "/path/2": None}


def test_hard_delete_does_not_corrupt_remaining_field_values(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"](
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"alpha":""'},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'},
        ]
    )
    storage.init_storage_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=10/minute=00/2026-05-22T10-00-00.svc.gz",
        [_row(base, 0, alpha="A0", beta="B0"), _row(base + timedelta(seconds=1), 1, alpha="A1", beta="B1")],
    )
    _ingest_and_commit(env, expected=2)

    env["set_custom_fields"]([{"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'}])
    if hasattr(storage, "_view_cache"):
        storage._view_cache.clear()
    storage.init_storage_table(env["fos_source"])

    env["seed_gz"](
        "raw/request/year=2026/month=05/day=22/hour=11/minute=00/2026-05-22T11-00-00.svc.gz",
        [_row(base + timedelta(hours=1), 2, beta="B2")],
    )
    _ingest_and_commit(env, expected=1)

    assert dict(_query_all(env, "url, beta")) == {"/path/0": "B0", "/path/1": "B1", "/path/2": "B2"}


def _cfg(custom_fields: list[dict]) -> dict:
    return {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields}


def test_disable_field_removes_from_ingest_hints_and_catalog_ids():
    cfg = _cfg(
        [
            {"name": "field_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "field_b", "duckdb_type": "BIGINT", "enabled": False},
        ]
    )
    for fn in (get_ingest_type_hints, get_catalog_field_ids):
        res = fn(cfg)
        assert "field_a" in res and "field_b" not in res


def test_disable_then_reenable_restores_ingest_hints():
    disabled = _cfg([{"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": False}])
    reenabled = _cfg([{"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": True}])
    assert "round_trip" not in get_ingest_type_hints(disabled)
    assert get_ingest_type_hints(reenabled).get("round_trip") == "VARCHAR"


def test_disable_does_not_affect_other_enabled_fields():
    cfg = _cfg(
        [
            {"name": "keep_me", "duckdb_type": "DOUBLE", "enabled": True},
            {"name": "drop_me", "duckdb_type": "BIGINT", "enabled": False},
            {"name": "also_keep", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    hints = get_ingest_type_hints(cfg)
    assert hints["keep_me"] == "DOUBLE" and hints["also_keep"] == "VARCHAR" and "drop_me" not in hints


def test_new_field_added_while_old_is_disabled_does_not_collide_on_name():
    cfg = _cfg(
        [
            {"name": "old_disabled", "duckdb_type": "VARCHAR", "enabled": False},
            {"name": "new_enabled", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    names = {f.name for f in storage.get_arrow_schema(cfg)}
    assert "new_enabled" in names and "old_disabled" not in names


def test_aggregation_over_all_null_column(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":0'}])
    storage.init_storage_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [_row(base + timedelta(seconds=i), i, score=None) for i in range(3)],
    )
    _ingest_and_commit(env, expected=3)

    con, view = _connect(env)
    n_rows, n_score, avg_score = con.execute(f"SELECT COUNT(*), COUNT(score), AVG(score) FROM {view}").fetchone()
    assert n_rows == 3 and n_score == 0 and avg_score is None


# ── End-to-end roundtrip tests ──
def _build_source(tmp_path, svc_id):
    return {
        "name": svc_id,
        "duckdb_path": str(tmp_path / f"{svc_id}.db"),
        "bucket": "mock_bucket",
        "prefix": "",
        "endpoint": "mock_endpoint",
        "region": "mock_region",
    }


def _admin_request():
    return SimpleNamespace(state=SimpleNamespace())


def _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir):
    monkeypatch.setattr(_bk_duckdb, "DUCKDB_PATH", source["duckdb_path"])
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(_bk_duckdb, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("name", "unknown")))
    monkeypatch.setattr(_bk_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(_bk_ingest, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(_bk_duckdb, "get_source_for_service", lambda x: None)
    monkeypatch.setattr(os, "remove", lambda *a: None)

    bucket_prefix = f"s3://{source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    _orig = _bk_duckdb.get_memory_connection

    class _Rewriter:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *a, **k):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    monkeypatch.setattr(_bk_duckdb, "get_memory_connection", lambda s: _Rewriter(_orig(s)))
    monkeypatch.setattr(_bk_ingest, "get_memory_connection", lambda s: _Rewriter(_orig(s)), raising=False)


def _ingest_once(monkeypatch, source, log_dir, key, payload, *, include_commit: bool = False):
    log_file = log_dir / key
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(log_file, "wt") as f:
        f.write(json.dumps(payload) + "\n")

    class _Fos:
        def get_paginator(self, *a, **k):
            return type("P", (), {"paginate": lambda self, **kw: [{"Contents": [{"Key": key, "Size": 100}]}]})()

        def get_object(self, Bucket, Key):
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **k):
            return {}

    monkeypatch.setattr(_bk_ingest, "_get_fos_client", lambda *a: _Fos())
    storage._view_cache.clear()
    _bk_duckdb._clear_schema_cache()
    _bk_duckdb._initialized_paths.clear()
    list(_bk_ingest.ingest(source=source))
    if include_commit:
        storage.commit_buffer(source)
    storage._view_cache.clear()
    _bk_duckdb._clear_schema_cache()
    _bk_duckdb._initialized_paths.clear()


def _open_view(source):
    con = _bk_duckdb.get_connection(source=source, skip_view_update=False)
    storage._view_cache.clear()
    storage.update_storage_view(con, source)
    return con


def _setup_roundtrip_svc(tmp_path, monkeypatch, svc_id: str, log_subdir: str, custom_fields: list[dict] | None = None):
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / log_subdir
    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    svcconfig.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "status": {},
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields or []},
        },
    )
    _bk_duckdb._ensure_source_registered(source)
    return source, log_dir


def test_custom_field_full_roundtrip_end_to_end(tmp_path, monkeypatch):
    svc_id = "roundtrip_svc"
    source, log_dir = _setup_roundtrip_svc(tmp_path, monkeypatch, svc_id, "mock_logs_rt")

    cf = CustomFieldCreate(
        name="my_field",
        label="My Field",
        vcl_log_expression="%{req.http.X-My-Field}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    services.api_create_custom_field(_admin_request(), svc_id, cf)

    custom = services_core_router.api_list_custom_fields(_admin_request(), svc_id).fields
    assert len(custom) == 1 and custom[0].name == "my_field" and custom[0].duckdb_type == "VARCHAR"

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/12/2026-05-10T12-00-00.test.gz",
        {"timestamp": "2026-05-10T12:00:00Z", "ip": "1.2.3.4", "status": 200, "my_field": "hello"},
    )

    con = _open_view(source)
    table_name = _bk_duckdb._safe_table_name(svc_id)
    assert "my_field" in {c["name"] for c in _bk_duckdb.get_schema(con, source)}
    assert con.execute(f"SELECT my_field FROM {table_name}").fetchall() == [("hello",)]


def test_custom_field_full_roundtrip_after_disable_enable_cycle(tmp_path, monkeypatch):
    svc_id = "roundtrip_cycle_svc"
    source, log_dir = _setup_roundtrip_svc(tmp_path, monkeypatch, svc_id, "mock_logs_cycle")

    cf = CustomFieldCreate(
        name="cycle_field",
        label="Cycle Field",
        vcl_log_expression="%{req.http.X-Cycle-Field}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    services.api_create_custom_field(_admin_request(), svc_id, cf)
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/12/2026-05-10T12-00-00.v1.gz",
        {"timestamp": "2026-05-10T12:00:00Z", "ip": "1.2.3.4", "status": 200, "cycle_field": "v1"},
    )

    for enabled in (False, True):
        services_core_router.api_update_custom_field(
            _admin_request(), svc_id, "cycle_field", CustomFieldUpdate(enabled=enabled)
        )
        cfg_fields = svcconfig.load_config(svc_id)["log_fields"]["custom_fields"]
        assert next(c for c in cfg_fields if c["name"] == "cycle_field")["enabled"] is enabled

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/13/2026-05-10T13-00-00.v2.gz",
        {"timestamp": "2026-05-10T13:00:00Z", "ip": "1.2.3.5", "status": 200, "cycle_field": "v2"},
    )

    con = _open_view(source)
    table_name = _bk_duckdb._safe_table_name(svc_id)
    values = [r[0] for r in con.execute(f"SELECT cycle_field FROM {table_name} ORDER BY timestamp").fetchall()]
    assert values == ["v1", "v2"]


def test_ingest_and_query_custom_fields(tmp_path, monkeypatch):
    svc_id = "custom_test_svc"
    source, log_dir = _setup_roundtrip_svc(
        tmp_path,
        monkeypatch,
        svc_id,
        "mock_logs_int",
        [
            {
                "name": "x_custom_id",
                "label": "Custom ID",
                "vcl_log_expression": "%{req.http.X-Custom-ID}V",
                "duckdb_type": "VARCHAR",
                "value_type": "string",
                "bytes_estimate": 36,
                "enabled": True,
            }
        ],
    )
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-01/00/2026-05-01T00-00-00.000-test.log.gz",
        {
            "timestamp": "2026-05-01T00:00:00Z",
            "ip": "1.2.3.4",
            "status": 200,
            "url": "/test",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 100,
            "elapsed": 5000,
            "x_custom_id": "abc-123-xyz",
        },
        include_commit=True,
    )

    con = _open_view(source)
    assert "x_custom_id" in {c["name"] for c in _bk_duckdb.get_schema(con, source)}
    assert con.execute(f"SELECT x_custom_id FROM {_bk_duckdb._safe_table_name(svc_id)}").fetchall() == [
        ("abc-123-xyz",)
    ]


def test_ingest_and_query_numeric_custom_fields(tmp_path, monkeypatch):
    svc_id = "numeric_custom_svc"
    source, log_dir = _setup_roundtrip_svc(
        tmp_path,
        monkeypatch,
        svc_id,
        "mock_logs_num",
        [
            {
                "name": "custom_int",
                "label": "Custom Int",
                "vcl_log_expression": "%{req.http.X-Int}V",
                "duckdb_type": "BIGINT",
                "value_type": "integer",
                "bytes_estimate": 8,
                "enabled": True,
            },
            {
                "name": "custom_float",
                "label": "Custom Float",
                "vcl_log_expression": "%{req.http.X-Float}V",
                "duckdb_type": "DOUBLE",
                "value_type": "float",
                "bytes_estimate": 10,
                "enabled": True,
            },
        ],
    )
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-01/00/2026-05-01T00-00-00.000-num.log.gz",
        {
            "timestamp": "2026-05-01T00:00:00Z",
            "ip": "1.2.3.4",
            "status": 200,
            "url": "/test",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 100,
            "elapsed": 5000,
            "custom_int": 42,
            "custom_float": 3.14159,
        },
        include_commit=True,
    )

    con = _open_view(source)
    schema_dict = {c["name"]: c["type"] for c in _bk_duckdb.get_schema(con, source)}
    assert schema_dict.get("custom_int") == "BIGINT" and schema_dict.get("custom_float") == "DOUBLE"
    table_name = _bk_duckdb._safe_table_name(svc_id)
    assert con.execute(f"SELECT custom_int * 2, ROUND(custom_float, 2) FROM {table_name}").fetchall() == [(84, 3.14)]


# ── Type mismatch resilience ──
def _run_type_mismatch_batch(env, field_def: dict, gz_key: str, values: list):
    env["set_custom_fields"]([field_def])
    storage.init_storage_table(env["fos_source"])
    base = datetime.now(UTC) - timedelta(hours=2)
    col = field_def["name"]
    env["seed_gz"](
        gz_key,
        [_row(base + timedelta(seconds=i), i, **{col: v}) for i, v in enumerate(values)],
    )
    _ingest_and_commit(env, expected=len(values))
    return dict(_query_all(env, f"url, {col}"))


def test_integer_field_with_string_value_nulls_only_that_cell(custom_field_env):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {
            "name": "user_score",
            "duckdb_type": "INTEGER",
            "enabled": True,
            "vcl": '"user_score":%{req.http.X-User-Score}V',
        },
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [42, "NaN", 17],
    )
    assert by_url == {"/path/0": 42, "/path/1": None, "/path/2": 17}


def test_integer_field_with_string_float_truncates_not_nulls(custom_field_env):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":1'},
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [1, "3.14"],
    )
    assert by_url == {"/path/0": 1, "/path/1": 3}


def test_boolean_field_with_arbitrary_string_nulls_the_cell(custom_field_env):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {"name": "is_bot", "duckdb_type": "BOOLEAN", "enabled": True, "vcl": '"is_bot":false'},
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [True, "maybe", False],
    )
    assert by_url == {"/path/0": True, "/path/1": None, "/path/2": False}


def test_batch_proceeds_when_one_file_has_only_type_mismatches(custom_field_env):
    env = custom_field_env
    env["set_custom_fields"]([{"name": "n", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"n":1'}])
    storage.init_storage_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.bad.gz",
        [_row(base + timedelta(seconds=i), i, n="bad") for i in range(3)],
    )
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=05/2026-05-20T10-05-00.good.gz",
        [_row(base + timedelta(minutes=5, seconds=i), 100 + i, n=50 + i) for i in range(3)],
    )
    _ingest_and_commit(env, expected=6)

    rows = _query_all(env, "url, n")
    assert len(rows) == 6
    assert sum(1 for _, v in rows if v is None) == 3
    assert sum(1 for _, v in rows if v is not None) == 3


@pytest.mark.parametrize(
    "ddb_type,bad_value,description",
    [
        ("BIGINT", "NaN", "BIGINT ← string 'NaN' → NULL"),
        ("BOOLEAN", "maybe", "BOOLEAN ← arbitrary string → NULL"),
        ("BIGINT", "not-a-number-at-all", "BIGINT ← clearly invalid string → NULL"),
        ("DOUBLE", "lol", "DOUBLE ← clearly invalid string → NULL"),
    ],
)
def test_typed_field_with_invalid_string_value_nulls_cell(custom_field_env, ddb_type, bad_value, description):
    by_url = _run_type_mismatch_batch(
        custom_field_env,
        {"name": "metric", "duckdb_type": ddb_type, "enabled": True, "vcl": '"metric":%{req.http.X-Metric}V'},
        "raw/request/year=2026/month=05/day=21/hour=10/minute=00/2026-05-21T10-00-00.matrix.gz",
        [1, bad_value, 2],
    )
    assert by_url["/path/1"] is None, f"contract violation ({description}): got {by_url['/path/1']!r}"


# ── Merged from tests/core/test_log_fields.py ──


def test_generate_log_format_and_field_overrides_and_default_off():
    fmt_minimal = generate_log_format({"groups": PRESETS["minimal"]["groups"], "field_overrides": {}})
    assert fmt_minimal.startswith("{") and fmt_minimal.endswith("}")
    assert '"timestamp":' in fmt_minimal and '"ip":' in fmt_minimal and '"url":' not in fmt_minimal

    fmt_standard = generate_log_format({"groups": PRESETS["standard"]["groups"], "field_overrides": {}})
    assert '"url":' in fmt_standard and '"pop":' in fmt_standard and '"country":' in fmt_standard

    fmt_ovr = generate_log_format({"groups": [], "field_overrides": {"url": True, "country": True}})
    assert '"url":' in fmt_ovr and '"country":' in fmt_ovr and '"pop":' not in fmt_ovr

    assert "cookie_session" in DEFAULT_OFF_FIELD_IDS
    enabled = resolve_enabled_fields({"groups": ["H"], "field_overrides": {}})
    assert "cookie_session" not in enabled and {"ja3", "ja4", "tls_ciphers_sha"} <= set(enabled)
    for preset_name in ("security", "full"):
        preset = PRESETS[preset_name]
        assert "cookie_session" not in resolve_enabled_fields(
            {"groups": preset["groups"], "field_overrides": preset.get("field_overrides", {})}
        )

    cfg_opt = {"groups": ["H"], "field_overrides": {"cookie_session": True}}
    assert "cookie_session" in resolve_enabled_fields(cfg_opt)
    assert '"cookie_session":' in generate_log_format(cfg_opt)
    assert "cookie_session" in get_required_edge_headers(cfg_opt)
    assert "cookie_session" in resolve_enabled_fields({"groups": [], "field_overrides": {"cookie_session": True}})
    assert "cookie_session" not in resolve_enabled_fields(
        {"groups": ["H"], "field_overrides": {"cookie_session": False}}
    )


def test_custom_origin_and_edge_fields_and_budget_clamps():
    cfg_orig = {
        "groups": ["L"],
        "custom_fields": [
            {
                "name": "bereq_drew",
                "vcl_log_expression": "beresp.http.x-drew",
                "collection_stage": "origin",
                "origin_log_frequency": "all",
                "enabled": True,
            }
        ],
    }
    deliver = generate_capture_vcl(cfg_orig)["deliver"]
    assert 'if (req.http.x-is-cluster-fetch != "1") {\n  unset resp.http.x-fos-origin-data:bereq_drew;\n}' in deliver
    assert "\nunset resp.http.x-fos-origin-data:bereq_drew;" not in deliver

    snippets_edge = generate_capture_vcl(
        {
            "groups": [],
            "custom_fields": [
                {
                    "name": "edge_randomint",
                    "vcl_log_expression": "randomint(1, 100)",
                    "collection_stage": "edge",
                    "enabled": True,
                }
            ],
        }
    )
    assert "edge_randomint" in snippets_edge["recv"]
    assert "edge_randomint" not in snippets_edge.get("deliver", "")

    cfg_num = {
        "groups": [],
        "custom_fields": [
            {
                "name": f"z_num_{i}",
                "vcl_log_expression": f"req.http.x-edge-score:n{i}",
                "collection_stage": "deliver",
                "value_type": "numeric",
                "enabled": True,
            }
            for i in range(8)
        ]
        + [
            {
                "name": "zzz_tail",
                "vcl_log_expression": "req.http.x-edge-score:tail",
                "collection_stage": "deliver",
                "value_type": "string",
                "enabled": True,
            }
        ],
    }
    fmt_num = generate_log_format(cfg_num)
    num_lens = [int(m) for m in re.findall(r"x-edge-score:n\d+, 0, (\d+)\)", fmt_num)]
    assert num_lens and all(n <= 20 for n in num_lens)
    tail = re.search(r"x-edge-score:tail, 0, (\d+)\)", fmt_num)
    assert tail and int(tail.group(1)) > 0

    fmt_cmcd = generate_log_format(
        {
            "groups": PRESETS["standard"]["groups"],
            "custom_fields": [dict(cf) for cf in _CMCD_CUSTOM_FIELDS] + [dict(cf) for cf in _SCORING_CUSTOM_FIELDS],
        }
    )
    assert int(re.search(r"x-edge-score:sid, 0, (\d+)\)", fmt_cmcd).group(1)) > 0
    assert int(re.search(r"x-edge-score:rtt, 0, (\d+)\)", fmt_cmcd).group(1)) > 0
    assert int(re.search(r"x-fos-edge-data:cmcd_sid, 0, (\d+)\)", fmt_cmcd).group(1)) < 2000

    fmt_ip = generate_log_format({"schema_version": 2, "groups": []})
    assert (
        'if(req.http.x-fos-edge-data:ip != "", req.http.x-fos-edge-data:ip, '
        'if(req.http.Fastly-Client-IP != "", req.http.Fastly-Client-IP, client.ip))' in fmt_ip
    )


def _valid_field(**overrides) -> dict:
    base = {
        "name": "my_field",
        "label": "My Field",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    ("overrides", "existing", "expected_substr"),
    [
        ({"name": "1bad"}, [], "lowercase alphanumeric"),
        ({"name": "bad-name"}, [], "lowercase alphanumeric"),
        ({"name": "select"}, [], "reserved word"),
        ({"name": "timestamp"}, [], "already a built-in"),
        ({"name": "my_field"}, ["my_field"], "already exists"),
        ({"label": ""}, [], "label"),
        ({"label": "x" * 81}, [], "label"),
        ({"description": "x" * 501}, [], "description"),
        ({"vcl_log_expression": ""}, [], "empty"),
        ({"vcl_log_expression": "x" * 513}, [], "512"),
        ({"vcl_log_expression": "req.url\n"}, [], "newlines"),
        ({"vcl_log_expression": "req.url; set req.x = 1"}, [], "semicolons"),
        ({"vcl_log_expression": "req.url // hack"}, [], "comments"),
        ({"vcl_log_expression": "req.url # hack"}, [], "comments"),
        ({"duckdb_type": "QUANTUM"}, [], "duckdb_type"),
        ({"value_type": "unknown"}, [], "value_type"),
        ({"duckdb_type": "INTEGER", "value_type": "ip"}, [], "not compatible"),
        ({"bytes_estimate": 0}, [], "bytes_estimate"),
        ({"bytes_estimate": 2000}, [], "bytes_estimate"),
        ({"bytes_estimate": "not-a-number"}, [], "bytes_estimate"),
        ({"collection_stage": "fetch"}, [], "collection_stage"),
        ({"bytes_estimate": 5}, [], "WARN:"),
    ],
)
def test_validate_custom_field_rules(overrides, existing, expected_substr):
    assert validate_custom_field(_valid_field(), []) == []
    missing_errors = validate_custom_field({"name": "x"}, [])
    assert any("Missing required field" in e for e in missing_errors)
    assert not any("label" in e and "characters" in e for e in missing_errors)

    errors = validate_custom_field(_valid_field(**overrides), existing)
    assert any(expected_substr in e for e in errors), f"Expected {expected_substr!r} in {errors}"


# ── Merged from tests/core/test_log_line_budget.py ──


def _make_custom_fields(count: int, bytes_each: int) -> list[dict]:
    return [
        {
            "name": f"cf{i:03d}",
            "label": f"cf{i:03d}",
            "description": "",
            "vcl_log_expression": "req.http.Host",
            "collection_stage": "edge",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": bytes_each,
            "nullable": True,
            "enabled": True,
            "show_in_dashboard": False,
            "show_in_logs": True,
            "filterable": True,
        }
        for i in range(count)
    ]


def test_log_line_budget_bands_and_clamp():
    assert lf.FASTLY_LOG_LINE_DELIVER_MAX == 16 * 1024
    assert 0.40 < lf.FASTLY_LOG_LINE_SAFE_MAX / lf.FASTLY_LOG_LINE_DELIVER_MAX < 0.80

    cfg_small = {"groups": ["A"], "field_overrides": {}}
    assert lf.estimate_log_line_bytes(cfg_small) < lf.FASTLY_LOG_LINE_SAFE_MAX
    assert lf.check_log_line_budget(cfg_small) is None

    cfg_warn = {
        "groups": ["A", "B", "C", "D"],
        "field_overrides": {},
        "custom_fields": _make_custom_fields(count=20, bytes_each=500),
    }
    est_warn = lf.estimate_log_line_bytes(cfg_warn)
    assert lf.FASTLY_LOG_LINE_SAFE_MAX <= est_warn < lf.FASTLY_LOG_LINE_DELIVER_MAX
    w = lf.check_log_line_budget(cfg_warn)
    assert w is not None and w["code"] == "LOG_LINE_APPROACHING_LIMIT" and w["severity"] == "warn"
    assert w["estimate_bytes"] == est_warn and str(est_warn) in w["message"] and "16" in w["message"]

    cfg_err = {"groups": [], "field_overrides": {}, "custom_fields": _make_custom_fields(count=20, bytes_each=1024)}
    est_err = lf.estimate_log_line_bytes(cfg_err)
    assert est_err >= lf.FASTLY_LOG_LINE_DELIVER_MAX
    e = lf.check_log_line_budget(cfg_err)
    assert e is not None and e["code"] == "LOG_LINE_TOO_LARGE" and e["severity"] == "error"
    assert "truncate" in e["message"].lower() and "16" in e["message"]

    cfg_clamp = {
        "field_limits": {"url": 4000, "ua": 4000, "referer": 4000},
        "custom_fields": _make_custom_fields(10, bytes_each=4000),
        "groups": {"core": True, "a": True},
    }
    for cf in cfg_clamp["custom_fields"]:
        cf["byte_limit"] = 4000
    fmt = lf.generate_log_format(cfg_clamp)
    caps = [int(m) for m in re.findall(r"substr\([^)]*, *0, *(\d+)\)", fmt)]
    assert caps and sum(caps) <= lf.FASTLY_LOG_LINE_DELIVER_MAX


def test_log_fields_endpoints_surface_warning_and_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    client = TestClient(_fastapi_app)

    svc_get = "test_svc_log_budget_get"
    config.save_config(
        svc_get,
        {
            "service_id": svc_get,
            "log_fields": {
                "schema_version": 2,
                "groups": ["A", "B", "C", "D"],
                "field_overrides": {},
                "custom_fields": _make_custom_fields(count=20, bytes_each=500),
            },
        },
    )
    resp_get = client.get(f"/api/services/{svc_get}/log-fields")
    assert (
        resp_get.status_code == 200 and resp_get.json()["line_budget_warning"]["code"] == "LOG_LINE_APPROACHING_LIMIT"
    )

    svc_post = "test_svc_log_budget_post"
    config.save_config(svc_post, {"service_id": svc_post, "log_fields": {"schema_version": 2, "groups": ["A"]}})
    payload = {
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B", "C", "D"],
            "custom_fields": _make_custom_fields(count=20, bytes_each=500),
        }
    }
    with patch("backend.core.metadata.base.export_admin_state", return_value=None):
        resp_post = client.post(f"/api/services/{svc_post}/log-fields", json=payload)
    assert (
        resp_post.status_code == 200 and resp_post.json()["line_budget_warning"]["code"] == "LOG_LINE_APPROACHING_LIMIT"
    )

    svc_noop = "test_svc_log_budget_noop"
    config.save_config(svc_noop, {"service_id": svc_noop, "log_fields": {"schema_version": 2, "groups": ["A"]}})
    cfg = config.load_config(svc_noop)
    cfg["log_fields"]["format_hash"] = lf.format_hash(cfg["log_fields"])
    config.save_config(svc_noop, cfg)
    resp_noop = client.post(f"/api/services/{svc_noop}/log-fields", json={"log_fields": cfg["log_fields"]})
    assert resp_noop.status_code == 200 and resp_noop.json().get("ok") is True


# ── Merged from tests/core/test_sql_ident.py ──
BACKEND = pathlib.Path(__file__).resolve().parents[2] / "backend"


@pytest.mark.parametrize("name", ["lake", "_x", "a1", "Logs_Foo", "client_vitals", "_"])
def test_accepts_bare_identifiers(name: str) -> None:
    assert sql_ident.is_safe_ident(name) is True
    assert sql_ident.require_safe_ident(name, "bad ident") == name


@pytest.mark.parametrize(
    "name",
    ["", "1abc", "a-b", 'a"b', "a b", "a;DROP", "a\n", "x.y", "é", "lake'--"],
)
def test_rejects_everything_else(name: str) -> None:
    assert sql_ident.is_safe_ident(name) is False
    with pytest.raises(ValueError, match="bad ident"):
        sql_ident.require_safe_ident(name, "bad ident")


def test_rejects_non_strings_and_error_message_shape() -> None:
    assert sql_ident.is_safe_ident(None) is False  # type: ignore[arg-type]
    assert sql_ident.is_safe_ident(5) is False  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        sql_ident.require_safe_ident(None, "bad ident")  # type: ignore[arg-type]
    with pytest.raises(ValueError) as exc:
        sql_ident.require_safe_ident("a-b", "invalid ducklake alias")
    assert str(exc.value) == "invalid ducklake alias: 'a-b'"


def test_bare_identifier_regex_has_one_owner() -> None:
    """No module besides sql_ident may declare the bare-identifier regex literal."""
    needles = ("[A-Za-z_][A-Za-z0-9_]*",)
    offenders = []
    for path in BACKEND.rglob("*.py"):
        if path.name in {"sql_ident.py", "sql_validator.py", "duckdb.py"}:
            continue
        text = path.read_text()
        if path.name in {"pg_connection.py", "clickhouse_client.py", "base.py", "clickhouse.py"}:
            continue
        if any(n in text for n in needles):
            offenders.append(str(path.relative_to(BACKEND)))
    assert offenders == [], f"use backend.core.sql_validator instead of a private identifier regex: {offenders}"
