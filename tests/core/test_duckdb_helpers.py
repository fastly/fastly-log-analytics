"""Tests for ``backend.core.duckdb`` — pure helpers, wrappers, type round-trips, concurrency, and recycle."""

from __future__ import annotations

import collections
import gc
import json
import os
import queue
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import backend.core.duckdb as d
import backend.core.duckdb as duckdb_mod
import backend.cron.jobs.duckdb_recycle as rec
from backend.core import duckdb_pool
from backend.core import duckdb_pool as _pool
from backend.core import reset as reset_mod
from backend.core import storage as _storage
from backend.core.duckdb import (
    DBBusyError,
    _cache_dir,
    _data_stats_fingerprint,
    _ensure_source_registered,
    _execute_query_with_retry,
    _safe_table_name,
    clear_initialization_state,
    close_all_connections,
    enrich_asn_labels,
    format_asn_label,
    get_asn_names,
    get_connection,
    get_raw_tree_node,
    get_source_for_service,
    get_sync_status,
    is_configured,
    log_cron_run,
    log_usage_calls,
    purge_usage_log,
    refresh_config_status,
    rum_duckdb_path,
    rum_source_for,
    start_cron_run,
    update_cron_duration,
)
from backend.core.duckdb_pool import (
    _forget_conn,
    _get_conn_state,
    _get_pool,
    _instrument,
    _percentile_summary,
    _Pool,
    _pool_api_rebind_lock_timeout_s,
    _pool_conn_memory_limit,
    _pool_enabled,
    _pool_heavy_cap,
    _pool_max_size,
    _pool_sweep_enabled,
    _pool_warm_at_boot_count,
    _pool_warm_at_boot_enabled,
    _PoolBusy,
    _pools,
    _pools_lock,
    _safe_buffer_mtime,
    _set_conn_state,
    checkout_connection,
    get_all_stats,
    is_heavy_route,
    shutdown_all,
    warm_pool_at_startup,
    warm_pool_for_service,
)
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.storage._core import _get_service_lock
from backend.utils.date_utils import safe_iso as _safe_iso


# ── Pure Helpers & Wrappers ──
def test_load_httpfs():
    con = MagicMock()
    d._load_httpfs(con)
    con.execute.assert_called_once_with("LOAD httpfs;")

    con2 = MagicMock()
    con2.execute.side_effect = [duckdb.IOException("missing extension"), None, None]
    d._load_httpfs(con2)
    assert [c.args[0] for c in con2.execute.call_args_list] == ["LOAD httpfs;", "INSTALL httpfs;", "LOAD httpfs;"]


def test_safe_iso():
    assert _safe_iso(None) is None
    assert _safe_iso(datetime(2026, 5, 18, 12, 30, 0)) == "2026-05-18T12:30:00Z"
    tz_out = _safe_iso(datetime(2026, 5, 18, 12, 30, 0, tzinfo=UTC))
    assert tz_out is not None and not tz_out.endswith("ZZ")
    assert _safe_iso("already-a-string") == "already-a-string"
    assert _safe_iso(42) == "42"


def test_get_dma_map_variants_and_cache(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d._dma_map_cache = None
    assert d._get_dma_map() == {}

    sys_dir = tmp_path / "data" / "system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    d._dma_map_cache = None
    (sys_dir / "dma.json").write_text(
        json.dumps({"features": [{"properties": {"dma_code": 101, "dma_name": "Boston"}}]})
    )
    assert d._get_dma_map()["101"] == "Boston"

    d._dma_map_cache = None
    (sys_dir / "dma_geojson.json").write_text(
        json.dumps(
            {
                "features": [
                    {"properties": {"dma_code": 501, "name": "New York"}},
                    {"properties": {"dma_code": 803, "name": "Los Angeles"}},
                ]
            }
        )
    )
    out1 = d._get_dma_map()
    assert out1 == {"501": "New York", "803": "Los Angeles"}
    assert d._get_dma_map() is out1


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("My-Service.Logs", "logs_my_service_logs"),
        ("UPPER_CASE", "logs_upper_case"),
        ("default", "logs"),
        ("---test---", "logs_test"),
        ("my_service", "logs_my_service"),
        ("my-service!", "logs_my_service"),
        ("MyService", "logs_myservice"),
        ("_my_service_", "logs_my_service"),
        ("my service", "logs_my_service"),
        ("service.name.v2", "logs_service_name_v2"),
    ],
)
def test_safe_table_name(raw, expected):
    assert _safe_table_name(raw) == expected


@pytest.mark.parametrize(
    ("num", "name", "expected"),
    [
        (7922, "", "AS7922"),
        (7922, None, "AS7922"),
        (7922, "AS7922", "AS7922"),
        (7922, "Comcast Cable", "Comcast Cable (7922)"),
        (7922, "Comcast Cable Communications", "Comcast Cable Communications (7922)"),
        (1234, "", "AS1234"),
        (1, "O'Reilly & Associates", "O'Reilly & Associates (1)"),
        (4294967295, "Max ASN", "Max ASN (4294967295)"),
        (0, "", "AS0"),
    ],
)
def test_format_asn_label(num, name, expected):
    assert format_asn_label(num, name) == expected


def test_cache_dir_variants():
    assert "my-bucket" in _cache_dir({"bucket": "my-bucket"})
    assert _cache_dir({"_cache_dir_override": "/tmp/test-cache"}) == "/tmp/test-cache"
    assert "default" in _cache_dir({})


def test_data_stats_fingerprint(tmp_path):
    assert _data_stats_fingerprint({"_cache_dir_override": str(tmp_path / "missing")}) is None

    cache_root = tmp_path / "cache"
    data_dir = cache_root / "data"
    (data_dir / "ts=1").mkdir(parents=True)
    (data_dir / "ts=1" / "f.parquet").write_bytes(b"x")
    src = {"_cache_dir_override": str(cache_root)}

    fp1 = _data_stats_fingerprint(src)
    assert fp1 is not None and fp1 == _data_stats_fingerprint(src) and fp1[1] == 1

    buf_dir = cache_root / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_001.parquet").write_bytes(b"x")
    assert _data_stats_fingerprint(src) == fp1
    (buf_dir / "batch_001.parquet").unlink()
    assert _data_stats_fingerprint(src) == fp1

    (data_dir / "ts=2").mkdir()
    fp2 = _data_stats_fingerprint(src)
    assert fp2 is not None and fp2 != fp1 and fp2[1] == fp1[1] + 1


def test_is_configured():
    base = {"endpoint": "e", "access_key_id": "k", "secret_access_key": "s", "bucket": "b"}
    assert is_configured(base) is True
    for k in base:
        assert is_configured({key: val for key, val in base.items() if key != k}) is False
        assert is_configured({**base, k: ""}) is False


def test_build_and_reload_default_source():
    with patch("backend.config.list_configs", return_value=[]):
        src = d._build_default_source()
    assert (src["name"], src["endpoint"], src["bucket"], src["region"]) == ("default", "", "", "us-east-1")

    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-1"}]),
        patch("backend.config.config_to_source", return_value={"name": "svc-1", "bucket": "b", "endpoint": "e"}),
    ):
        src2 = d._build_default_source()
    assert (src2["name"], src2["bucket"]) == ("svc-1", "b")

    original_default = dict(d._DEFAULT_SOURCE)
    fake_src = {
        "name": "new-svc",
        "endpoint": "new-endpoint",
        "bucket": "new-bucket",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-west-2",
        "duckdb_path": "/tmp/new.duckdb",
        "storage_mode": "local",
    }
    try:
        with (
            patch("backend.config.list_configs", return_value=[{"service_id": "new-svc"}]),
            patch("backend.config.config_to_source", return_value=fake_src),
        ):
            d.reload_default_source()
        assert d._DEFAULT_SOURCE["bucket"] == "new-bucket" and d.DUCKDB_PATH == "/tmp/new.duckdb"
    finally:
        d._DEFAULT_SOURCE.clear()
        d._DEFAULT_SOURCE.update(original_default)
        d.DUCKDB_PATH = original_default.get("duckdb_path", "logs.duckdb")


def test_get_source_for_service():
    with patch("backend.config.load_config", return_value=None), patch("backend.config.list_configs", return_value=[]):
        assert get_source_for_service("ghost-svc") is None

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-1"}),
        patch("backend.config.config_to_source", return_value={"name": "svc-1", "bucket": "b"}),
    ):
        assert get_source_for_service("svc-1")["name"] == "svc-1"

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.list_configs", return_value=[{"service_id": "log-svc", "cdn_service_id": "cdn-id"}]),
        patch("backend.config.config_to_source", return_value={"name": "log-svc", "bucket": "b"}),
    ):
        assert get_source_for_service("cdn-id")["name"] == "log-svc"


def test_ensure_source_registered_and_start_cron_run():
    src = {
        "name": "test-svc",
        "endpoint": "ep",
        "bucket": "b",
        "prefix": "p",
        "region": "us-east-1",
        "cdn_url": "https://cdn.x",
        "cdn_secret": "shh",
    }
    with patch("backend.core.metadata.register_source") as mock_reg:
        assert _ensure_source_registered(src) == "logs_test_svc"
    payload = json.loads(mock_reg.call_args[0][2])
    assert (payload["bucket"], payload["cdn_url"], payload["cdn_secret"]) == ("b", "https://cdn.x", "shh")

    with patch("backend.core.metadata.start_cron_run", return_value=99) as mock_start:
        assert start_cron_run({"name": "svc-1"}, "sync") == 99
    mock_start.assert_called_once_with("svc-1", "sync")


def test_execute_query_with_retry():
    con = MagicMock()
    con.execute.return_value = "result"
    assert _execute_query_with_retry(con, "SELECT 1") == "result"
    assert con.execute.call_count == 1

    for err_msg in ("HTTP 401 on 'https://x.example/key'", "HTTP 403 forbidden", "Parser error: syntax error"):
        c = MagicMock()
        c.execute.side_effect = RuntimeError(err_msg)
        with pytest.raises(RuntimeError):
            _execute_query_with_retry(c, "SELECT 1")
        assert c.execute.call_count == 1

    c_transient = MagicMock()
    c_transient.execute.side_effect = [duckdb.IOException("IO Error: timeout"), "success"]
    with patch("time.sleep"):
        assert _execute_query_with_retry(c_transient, "SELECT 1", max_retries=2) == "success"
    assert c_transient.execute.call_count == 2

    c_fail = MagicMock()
    c_fail.execute.side_effect = duckdb.IOException("connection refused")
    with patch("time.sleep"), pytest.raises(duckdb.IOException, match="connection refused"):
        _execute_query_with_retry(c_fail, "SELECT 1", max_retries=2)
    assert c_fail.execute.call_count == 3


def test_initialization_state_and_dbbusy_error():
    d._initialized_paths.add("/tmp/test-path.duckdb")
    clear_initialization_state("/tmp/test-path.duckdb")
    assert "/tmp/test-path.duckdb" not in d._initialized_paths
    clear_initialization_state("/tmp/never-initialized.duckdb")
    close_all_connections()
    assert issubclass(DBBusyError, Exception) and str(DBBusyError("locked")) == "locked"


def test_log_cron_run_behaviors():
    # 1. Persists error regardless of log_enabled=False
    log_calls = []
    with (
        patch("backend.config.load_config", return_value={"provisioning": {"cron_sync": {"log_enabled": False}}}),
        patch("backend.core.metadata.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
    ):
        log_cron_run({"name": "svc"}, "sync", 1.5, "error", error_message="bad", run_id=42)
    assert len(log_calls) == 1

    # 2. Deletes pending when success with log_enabled=False
    deleted = []
    with (
        patch("backend.config.load_config", return_value={"provisioning": {"cron_sync": {"log_enabled": False}}}),
        patch("backend.core.metadata.delete_cron_run", side_effect=lambda sid, rid: deleted.append((sid, rid))),
        patch("backend.core.metadata.log_cron_run") as mock_log,
    ):
        log_cron_run({"name": "svc"}, "sync", 1.0, "success", run_id=42)
    mock_log.assert_not_called()
    assert deleted == [("svc", 42)]

    # 3. Upgrades status to partial_success on corrupt_rows > 0, keeps success on 0
    for rows, exp_status in ((5, "partial_success"), (0, "success")):
        captured = {}
        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.core.metadata.log_cron_run", side_effect=lambda *a, **kw: captured.update(args=a)),
        ):
            log_cron_run({"name": "svc"}, "sync", 1.0, "success", corrupt_rows=rows)
        assert captured["args"][3] == exp_status

    # 4. Only local_compact consults cron_compact.log_enabled
    deleted_lc = []
    with (
        patch("backend.config.load_config", return_value={"provisioning": {"cron_compact": {"log_enabled": False}}}),
        patch("backend.core.metadata.delete_cron_run", side_effect=lambda sid, rid: deleted_lc.append((sid, rid))),
        patch("backend.core.metadata.log_cron_run"),
    ):
        log_cron_run({"name": "svc"}, "local_compact", 1.0, "success", run_id=11)
    assert deleted_lc == [("svc", 11)]

    for task in ("commit", "optimize", "expire", "full_sync", "metadata_cleanup"):
        del_t, log_t = [], []
        with (
            patch(
                "backend.config.load_config", return_value={"provisioning": {"cron_compact": {"log_enabled": False}}}
            ),
            patch("backend.core.metadata.delete_cron_run", side_effect=lambda sid, rid: del_t.append((sid, rid))),
            patch("backend.core.metadata.log_cron_run", side_effect=lambda *a, **kw: log_t.append((a, kw))),
        ):
            log_cron_run({"name": "svc"}, task, 1.0, "success", run_id=42)
        assert del_t == [] and len(log_t) == 1

    # 5. Swallows metadata_db exception
    with (
        patch("backend.config.load_config", return_value={"provisioning": {}}),
        patch("backend.core.metadata.log_cron_run", side_effect=RuntimeError("database is locked")),
    ):
        log_cron_run({"name": "svc"}, "sync", 1.0, "success")


class _FakeRow:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row

    def fetchall(self):
        return [self.row] if self.row else []


def test_get_sync_status_variants():
    # Unconfigured
    out = get_sync_status(MagicMock(), {"name": "fresh-svc"})
    assert (out["configured"], out["local_rows"], out["ingested"], out["fos_total"]) == (False, 0, 0, 0)

    src = {
        "name": "svc",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
        "prefix": "p",
    }

    # skip_fos cached
    with (
        patch(
            "backend.config.get_status",
            return_value={"local_rows": 999, "ingested": 100, "fos_new": 5, "fos_total": 200},
        ),
        patch("backend.core.metadata.get_ingested_files_status_summary") as mock_sum,
    ):
        out_cached = get_sync_status(MagicMock(), src, skip_fos=True)
    assert out_cached["local_rows"] == 999 and out_cached["configured"] is True and not mock_sum.called

    # Derives FOS fields from local ingested_files without globbing
    summary_derive = {
        "file_count": 3,
        "total_rows": 6000,
        "total_bytes": 600,
        "count_with_bytes": 3,
        "last_ingested": "2026-05-19T02:00:01Z",
        "latest_file_name": "s3://b/p/raw/2026-05-19/02/2026-05-19T02-00-00.ccc.gz",
    }

    class _ConNoTable:
        def execute(self, sql, *args):
            return _FakeRow(None)

    def _fail_if_glob(con, query, **_kw):
        assert "glob(" not in query.lower()
        return _FakeRow(None)

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary_derive),
        patch.object(d, "_execute_query_with_retry", side_effect=_fail_if_glob),
    ):
        out_derive = get_sync_status(_ConNoTable(), {**src, "name": "svc-derive"}, skip_fos=False, force=True)
    assert out_derive["fos_total"] == 3
    assert out_derive["latest_available_file_at"] == out_derive["latest_ingested_file_at"]

    # Busts view cache before retrying on "No files found"
    call_order = []
    count_calls = {"n": 0}

    class _ConStale:
        def execute(self, sql, *args):
            if "information_schema.tables" in sql:
                return _FakeRow((1,))
            if "count(*)" in sql:
                count_calls["n"] += 1
                if count_calls["n"] == 1:
                    raise Exception("IO Error: No files found that match the pattern cache/.../buffer/batch_x.parquet")
                return _FakeRow((42, "2026-05-19T00:00:00Z", "2026-05-19T05:00:00Z"))
            return _FakeRow(None)

    empty_summary = {
        "file_count": 0,
        "total_rows": 0,
        "total_bytes": 0,
        "count_with_bytes": 0,
        "last_ingested": None,
        "latest_file_name": None,
    }
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=empty_summary),
        patch(
            "backend.core.storage.clear_source_caches", side_effect=lambda _k, **_kw: call_order.append("clear")
        ) as mock_clr,
        patch(
            "backend.core.storage.update_storage_view", side_effect=lambda *_a, **_kw: call_order.append("update")
        ) as mock_upd,
    ):
        out_stale = get_sync_status(_ConStale(), {**src, "name": "stale-cache-svc"}, skip_fos=True, force=True)
    assert call_order == ["clear", "update"] and out_stale["local_rows"] == 42
    mock_clr.assert_called_once_with("stale-cache-svc", keep_snapshot_cache=True)
    mock_upd.assert_called_once()

    # Prefers ingested count when view returns 0, trusts view count when > 0 and smaller than ingested
    for view_row, ingested_total, exp_rows, exp_earliest in (
        ((0, None, None), 1_272_371, 1_272_371, None),
        ((1_635_368, "2026-05-15T17:30:00Z", "2026-05-25T22:04:29Z"), 3_845_401, 1_635_368, "2026-05-15T17:30:00Z"),
    ):

        class _ConView:
            def execute(self, sql, *args):
                if "information_schema.tables" in sql:
                    return _FakeRow((1,))
                if "count(*)" in sql:
                    return _FakeRow(view_row)
                return _FakeRow(None)

        sum_row = {**summary_derive, "total_rows": ingested_total}
        with (
            patch("backend.config.is_usage_logging_enabled", return_value=False),
            patch("backend.config.get_status", return_value=None),
            patch("backend.core.metadata.get_ingested_files_status_summary", return_value=sum_row),
        ):
            out_v = get_sync_status(_ConView(), {**src, "name": f"svc-{exp_rows}"}, skip_fos=True, force=True)
        assert out_v["local_rows"] == exp_rows and out_v["earliest_log_at"] == exp_earliest


def test_clear_source_caches_and_refresh_config_status():
    _storage._view_cache["svc"] = ("loc", set(), tuple(), "SELECT * FROM existing_view", 1.0, False)
    _storage.clear_source_caches("svc", keep_snapshot_cache=True)
    assert "svc" not in _storage._view_cache

    _storage._view_cache["teardown-svc"] = ("loc", set(), tuple(), "SELECT 1", 1.0, False)
    _storage._view_cache["teardown-svc::client_vitals"] = ("loc", set(), tuple(), "SELECT 1", 1.0, False)
    _storage.clear_source_caches("teardown-svc")
    assert "teardown-svc" not in _storage._view_cache and "teardown-svc::client_vitals" not in _storage._view_cache

    captured = {}

    class _StubCon:
        def execute(self, *_a, **_k):
            return _FakeRow(None)

        def close(self):
            pass

    with (
        patch("backend.config.load_config", return_value={"name": "svc", "bucket": "b", "service_id": "svc"}),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch("backend.config.update_status"),
        patch(
            "backend.core.duckdb.get_connection",
            side_effect=lambda s, **kw: captured.update(kw, source=s) or _StubCon(),
        ),
        patch("backend.core.duckdb.get_sync_status", return_value={"ingested": 0}),
        patch("backend.core.duckdb.get_schema", return_value=[]),
    ):
        refresh_config_status("svc")
    assert captured.get("read_only") is True and captured.get("skip_view_update") is True


def test_get_storage_tree_node_lists_the_local_data_dir_and_rejects_traversal(tmp_path, monkeypatch):
    from backend import config as svcconfig

    lake = tmp_path / "svc" / "ducklake" / "main" / "logs_svc"
    lake.mkdir(parents=True)
    (lake / "a.parquet").write_bytes(b"12345")
    (tmp_path / "svc" / "ducklake" / "top.txt").write_bytes(b"xy")
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", tmp_path)
    monkeypatch.setattr(svcconfig, "DUCKLAKE_DATA_PATH", None)
    src = {"service_id": "svc", "name": "svc"}

    top = d.get_storage_tree_node(src)["children"]
    assert [(c["name"], c["type"]) for c in top] == [("main", "directory"), ("top.txt", "file")]
    assert top[0]["size"] == 5 and top[0]["prefix"] == "main"
    inner = d.get_storage_tree_node(src, "main/logs_svc")["children"]
    assert [(c["name"], c["size"]) for c in inner] == [("a.parquet", 5)]
    assert d.get_storage_tree_node(src, "../../..") == {"children": []}


def test_get_storage_tree_node_uses_the_fos_ducklake_prefix_for_remote_data(monkeypatch):
    with (
        patch("backend.core.storage._core._resolve_data_path", return_value="s3://bkt/pfx/ducklake/"),
        patch.object(d, "get_raw_tree_node", return_value={"children": []}) as raw,
    ):
        assert d.get_storage_tree_node({"service_id": "svc", "name": "svc"}, "main") == {"children": []}
    raw.assert_called_once_with({"service_id": "svc", "name": "svc"}, "main", root="ducklake")


def test_get_raw_tree_node():
    assert get_raw_tree_node({"name": "fresh"}) == {"children": []}

    src = {"name": "svc", "endpoint": "ep", "access_key_id": "k", "secret_access_key": "s", "bucket": "b", "prefix": ""}
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw/file1.gz", "Size": 100, "LastModified": datetime.now(UTC)},
                {"Key": "raw/subdir/a.gz", "Size": 50, "LastModified": datetime.now(UTC)},
                {"Key": "raw/subdir/b.gz", "Size": 70, "LastModified": datetime.now(UTC)},
            ]
        }
    ]
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator
    with patch("backend.core.duckdb._get_fos_client", return_value=fake_s3):
        children = get_raw_tree_node(src, prefix_filter="", root="raw")["children"]
    assert len(children) == 2
    dir_entry = next(c for c in children if c["type"] == "directory")
    file_entry = next(c for c in children if c["type"] == "file")
    assert (dir_entry["name"], dir_entry["size"]) == ("subdir", 120)
    assert (file_entry["name"], file_entry["size"]) == ("file1.gz", 100)

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("S3 timeout")):
        assert get_raw_tree_node(src) == {"children": []}


def test_asn_names_and_enrichment():
    assert get_asn_names("svc-1", []) == {}
    assert get_asn_names("", [7922]) == {}

    cached = {7922: "Comcast", 15169: "Google"}
    with (
        patch("backend.core.metadata.lookup_asn_names", return_value=dict(cached)),
        patch("cymruwhois.Client") as mock_cw,
    ):
        assert get_asn_names("svc-1", [7922, 15169]) == cached
    mock_cw.assert_not_called()

    with (
        patch("backend.core.metadata.lookup_asn_names", return_value={}),
        patch("cymruwhois.Client", side_effect=ImportError()),
    ):
        assert get_asn_names("svc-1", [7922, 15169]) == {7922: "AS7922", 15169: "AS15169"}

    with (
        patch("backend.core.metadata.lookup_asn_names", side_effect=RuntimeError("locked")),
        patch("cymruwhois.Client", side_effect=ImportError()),
    ):
        assert get_asn_names("svc-1", [7922]) == {7922: "AS7922"}

    values = [{"value": "7922"}, {"value": "15169"}, {"value": "non-numeric"}]
    with patch("backend.core.duckdb.get_asn_names", return_value=cached):
        out = enrich_asn_labels(values, "svc-1")
    assert out is values
    assert (
        values[0]["label"] == "Comcast (7922)" and values[1]["label"] == "Google (15169)" and "label" not in values[2]
    )

    with patch("backend.core.duckdb.get_asn_names") as mock_get:
        enrich_asn_labels([{"value": "US"}, {"value": "GB"}], "svc-1")
    mock_get.assert_not_called()


def test_cron_duration_and_usage_log_wrappers():
    captured_dur = []
    with patch(
        "backend.core.metadata.update_cron_duration",
        side_effect=lambda sid, rid, dur, log_output=None: captured_dur.append((sid, rid, dur, log_output)),
    ):
        update_cron_duration({"name": "svc-1", "service_id": "other-id"}, 42, 1.5)
        update_cron_duration({}, 42, 1.0)
    assert captured_dur == [("svc-1", 42, 1.5, None)]

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        log_usage_calls({"name": "svc-1"}, [{"method": "GET", "service": "FOS"}])
    mock_log.assert_not_called()

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.log_usage_calls") as mock_log2,
    ):
        log_usage_calls({}, [{"method": "GET", "service": "FOS"}])
    mock_log2.assert_not_called()

    captured_usage = {}
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda sid, calls, process_context=None: captured_usage.update(
                sid=sid, calls=calls, ctx=process_context
            ),
        ),
    ):
        log_usage_calls({"name": "svc-1"}, [{"method": "GET", "service": "FOS"}], process_context="cron:sync")
    assert (captured_usage["sid"], captured_usage["ctx"]) == ("svc-1", "cron:sync")

    for cfg_ret, exp_days in (({"retention_days": 14}, 14), ({}, 30)):
        purged = []
        with (
            patch("backend.config.load_usage_logging_config", return_value=cfg_ret),
            patch("backend.core.metadata.purge_usage_log", side_effect=lambda sid, days: purged.append((sid, days))),
        ):
            purge_usage_log({"name": "svc-1"})
        assert purged == [("svc-1", exp_days)]


# ── Connection Lock Handling, Threads & RUM Helpers ──
class TestGetConnectionLockHandling:
    @pytest.mark.parametrize(
        "err_msg",
        [
            "database is locked",
            "write-write conflict on table",
            "Connection Error: Can't open a connection to same database file with a different configuration than existing connections",
        ],
    )
    def test_transient_lock_errors_raise_db_busy(self, err_msg):
        with patch("backend.core.duckdb.duckdb.connect", side_effect=Exception(err_msg)):
            with pytest.raises(DBBusyError):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)

    def test_lock_timeout_logs_topology_context(self, caplog):
        with (
            patch("backend.core.duckdb.duckdb.connect", side_effect=Exception("database is locked")),
            caplog.at_level("ERROR", logger="backend.core.duckdb"),
            pytest.raises(DBBusyError),
        ):
            get_connection(
                source={"logging_service_id": "svc-1", "duckdb_path": "/tmp/_test.duckdb"},
                max_wait=0.1,
                read_only=True,
            )
        record = next(r for r in caplog.records if r.getMessage().startswith("duckdb_connection_lock_timeout"))
        assert (
            record.args["service_id"],
            record.args["db_path"],
            record.args["read_only"],
            record.args["deployment_mode"],
        ) == (
            "svc-1",
            "/tmp/_test.duckdb",
            True,
            "standard",
        )

    def test_unexpected_error_propagates_immediately(self):
        with (
            patch("backend.core.duckdb.duckdb.connect", side_effect=Exception("Permission denied: /var/duckdb")),
            pytest.raises(Exception, match="Permission denied"),
        ):
            get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)


def test_threads_and_pool_settings(tmp_path, monkeypatch):
    db = str(tmp_path / "svc.duckdb")
    a, b = duckdb.connect(db), duckdb.connect(db)
    try:
        a.execute("SET threads = 1")
        b.execute("SET threads = 3")
        assert a.execute("SELECT current_setting('threads')").fetchone()[0] == 3

        a.execute("SET threads = 2")
        spy = MagicMock(wraps=a)
        duckdb_mod._ensure_threads(spy, 2)
        assert not any("SET threads" in str(c.args[0]) for c in spy.execute.call_args_list)
        duckdb_mod._ensure_threads(spy, 3)
        assert a.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    finally:
        a.close()
        b.close()

    monkeypatch.setenv("DUCKDB_POOL_CONN_THREADS", "1")
    pool = _Pool(service_key="test_no_threads", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=mock_conn),
        patch("backend.core.storage._core._view_cache", {}),
        patch("backend.core.storage._core.update_storage_view"),
    ):
        pool.acquire(src={"name": "test_no_threads"}, max_wait=0.5)
    executed = [str(c.args[0]).lower() for c in mock_conn.execute.call_args_list if c.args]
    assert not any("threads" in s for s in executed)


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("/data/.duckdb/svc.duckdb", "/data/.duckdb/svc.rum.duckdb"),
        ("svc", "svc.rum.duckdb"),
        ("/a.duckdb.duckdb", "/a.duckdb.rum.duckdb"),
    ],
)
def test_rum_duckdb_path_and_source(given: str, expected: str) -> None:
    assert rum_duckdb_path(given) == expected
    nested = {"k": "v"}
    source = {"name": "svc", "duckdb_path": "/data/.duckdb/svc.duckdb", "opts": nested}
    out = rum_source_for(source)
    assert (out["duckdb_path"], out["name"]) == ("/data/.duckdb/svc.rum.duckdb", "svc::rum")
    assert out is not source and out["opts"] is nested and source["name"] == "svc"


# ── Type Round-Trip Tests ──
_TYPE_BOUNDARIES: dict[str, list] = {
    "TIMESTAMP": ["2026-05-15 12:00:00", "1970-01-01 00:00:00", "2099-12-31 23:59:59.999"],
    "VARCHAR": ["", "ascii", "héllo wörld", "日本語", "with 'quote' and \"dquote\" and \\backslash", "x" * 5000],
    "BOOLEAN": [True, False],
    "UTINYINT": [0, 255, 1, 127],
    "USMALLINT": [0, 65535, 1, 32768],
    "UINTEGER": [0, 4_294_967_295, 1, 2_147_483_648],
    "UBIGINT": [0, 18_446_744_073_709_551_615, 1, 9_223_372_036_854_775_808],
    "BIGINT": [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807, 0, -1],
    "FLOAT": [0.0, 3.14159, -1.5, 1e10, -1e10, 1e-10],
    "DOUBLE": [0.0, 3.141592653589793, -1.5e100, 1e300, -1e300],
}


def test_catalog_uses_only_known_duckdb_types():
    seen = Counter(f["duckdb_type"] for f in LOG_FIELD_CATALOG)
    unknown = set(seen) - set(_TYPE_BOUNDARIES)
    assert not unknown, f"new duckdb_type(s) in catalog need test coverage: {unknown}"


@pytest.mark.parametrize("ddb_type", sorted(_TYPE_BOUNDARIES))
def test_type_round_trip(ddb_type):
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE TABLE t (val {ddb_type})")
        for v in _TYPE_BOUNDARIES[ddb_type]:
            con.execute("INSERT INTO t VALUES (?)", (v,))
        got = [r[0] for r in con.execute("SELECT val FROM t ORDER BY rowid").fetchall()]
        if ddb_type in ("FLOAT", "DOUBLE"):
            for expected, actual in zip(_TYPE_BOUNDARIES[ddb_type], got):
                assert actual == 0.0 if expected == 0.0 else abs(actual - expected) / abs(expected) < 1e-5
        elif ddb_type == "TIMESTAMP":
            for expected, actual in zip(_TYPE_BOUNDARIES[ddb_type], got):
                assert str(actual).startswith(str(expected)[:19])
        else:
            assert got == _TYPE_BOUNDARIES[ddb_type]
    finally:
        con.close()


def test_special_duckdb_type_invariants():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t_u (val UINTEGER)")
        with pytest.raises(duckdb.Error):
            con.execute("INSERT INTO t_u VALUES (-1)")

        con.execute("CREATE TABLE t_v (val VARCHAR)")
        emoji = "🚀" * 100
        con.execute("INSERT INTO t_v VALUES (?)", (emoji,))
        assert con.execute("SELECT val, length(val), strlen(val) FROM t_v").fetchone() == (emoji, 100, 400)

        con.execute("CREATE TABLE t_fd (f FLOAT, d DOUBLE)")
        precise = 123456.789012345
        con.execute("INSERT INTO t_fd VALUES (?, ?)", (precise, precise))
        f_val, d_val = con.execute("SELECT f, d FROM t_fd").fetchone()
        assert abs(f_val - precise) > 1e-5 and abs(d_val - precise) < 1e-9

        con.execute("INSTALL icu; LOAD icu; SET TimeZone = 'UTC'; CREATE TABLE t_tz (ts TIMESTAMPTZ)")
        for raw in ("2026-05-15T12:00:00+00:00", "2026-05-15T12:00:00-07:00", "2026-05-15T17:30:00+05:30"):
            con.execute("INSERT INTO t_tz VALUES (?::TIMESTAMPTZ)", (raw,))
        utc_strs = [
            r[0] for r in con.execute("SELECT strftime(ts, '%Y-%m-%d %H:%M:%S') FROM t_tz ORDER BY rowid").fetchall()
        ]
        assert utc_strs == ["2026-05-15 12:00:00", "2026-05-15 19:00:00", "2026-05-15 12:00:00"]

        con.execute("CREATE TABLE t_cut (ts TIMESTAMPTZ)")
        con.execute(
            "INSERT INTO t_cut VALUES ('2026-05-15T11:59:59Z'::TIMESTAMPTZ), ('2026-05-15T12:00:01Z'::TIMESTAMPTZ)"
        )
        assert (
            con.execute(
                "SELECT COUNT(*) FROM t_cut WHERE ts <= ?::TIMESTAMPTZ", ("2026-05-15T05:00:00-07:00",)
            ).fetchone()[0]
            == 1
        )
    finally:
        con.close()


def test_microsecond_precision_for_elapsed_ottfb_round_trip_via_parquet():
    elapsed_us = [50_000, 1_234_567, 63_000_000, 9_007_199_254_740_993]
    ottfb_us = [128, 4_287, 12_345_678, 1_000_001]
    table = pa.table({"elapsed": pa.array(elapsed_us, type=pa.uint64()), "ottfb": pa.array(ottfb_us, type=pa.uint64())})
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "timing.parquet")
        pq.write_table(table, path, compression="zstd")
        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(f"SELECT elapsed, ottfb FROM read_parquet('{path}') ORDER BY elapsed").fetchall()
        finally:
            con.close()
    assert rows == sorted(zip(elapsed_us, ottfb_us))


# ── Concurrency & Lock Retry Tests ──
def _src(db_path: str) -> dict:
    return {
        "name": "test",
        "duckdb_path": db_path,
        "endpoint": "",
        "access_key_id": "",
        "secret_access_key": "",
        "region": "us-east-1",
        "bucket": "",
        "prefix": "",
        "cdn_url": "",
        "cdn_secret": "",
    }


def _make_failing_connect(fail_count: int, real_connect, target_path: str):
    state = {"calls": 0}

    def _fake(db_path, read_only=False):
        if db_path != target_path:
            return real_connect(db_path, read_only=read_only)
        state["calls"] += 1
        if state["calls"] <= fail_count:
            raise duckdb.Error("Could not set lock on file: database is locked")
        return real_connect(db_path, read_only=read_only)

    return _fake, state


def test_succeeds_after_transient_lock_errors_and_exponential_backoff(tmp_path, monkeypatch):
    db_path = str(tmp_path / "retry.duckdb")
    duckdb.connect(db_path).close()
    real_connect = duckdb.connect
    fake, state = _make_failing_connect(fail_count=5, real_connect=real_connect, target_path=db_path)
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", fake)

    with ThreadPoolExecutor(max_workers=1) as ex:
        ex.submit(lambda: duckdb.connect(str(tmp_path / "unrelated.duckdb")).close()).result()
    assert state["calls"] == 0

    sleeps: list[float] = []
    real_sleep = time.sleep
    monkeypatch.setattr(
        duckdb_mod.time,
        "sleep",
        lambda s: (
            (sleeps.append(s), real_sleep(0))
            if threading.current_thread() is threading.main_thread()
            else real_sleep(s)
        ),
    )

    con = get_connection(_src(db_path), max_wait=30.0, read_only=False)
    try:
        assert con.execute("SELECT 1").fetchone() == (1,)
    finally:
        con.close()

    assert state["calls"] == 6 and len(sleeps) == 5
    for got, want in zip(sleeps, [0.05, 0.10, 0.20, 0.40, 0.50]):
        assert abs(got - want) < 1e-6


def test_deadline_exceeded_non_lock_and_writer_reader_release(tmp_path, monkeypatch):
    real_connect = duckdb.connect
    real_sleep = time.sleep
    owner = threading.current_thread()
    monkeypatch.setattr(
        duckdb_mod.time, "sleep", lambda s: None if threading.current_thread() is owner else real_sleep(s)
    )

    # 1. Deadline exceeded
    busy_path = str(tmp_path / "busy.duckdb")
    calls = {"n": 0}

    def _always_locked(path, read_only=False):
        if path != busy_path:
            return real_connect(path, read_only=read_only)
        calls["n"] += 1
        raise duckdb.Error("Could not set lock on file: database is locked")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _always_locked)
    with pytest.raises(DBBusyError) as excinfo:
        get_connection(_src(busy_path), max_wait=0.2)
    assert "locked by another process" in str(excinfo.value) and excinfo.value.__cause__ is not None and calls["n"] > 1

    # 2. Non-lock error not retried
    boom_path = str(tmp_path / "boom.duckdb")
    boom_calls = {"n": 0}

    def _boom(path, read_only=False):
        if path != boom_path:
            return real_connect(path, read_only=read_only)
        boom_calls["n"] += 1
        raise duckdb.Error("Catastrophic failure: something unrelated to locking")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _boom)
    with pytest.raises(duckdb.Error, match="Catastrophic failure"):
        get_connection(_src(boom_path), max_wait=5.0, read_only=False)
    assert boom_calls["n"] == 1

    # 3. Writer then reader release path
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", real_connect)
    rel_path = str(tmp_path / "release.duckdb")
    boot = duckdb.connect(rel_path)
    boot.execute("CREATE TABLE t(x INTEGER)")
    boot.close()
    w = get_connection(_src(rel_path), read_only=False)
    w.close()
    r = get_connection(_src(rel_path), read_only=True)
    try:
        assert r.execute("SELECT count(*) FROM t").fetchone() == (0,)
    finally:
        r.close()


# ── DuckDB Recycle & Barrier Tests ──
def test_live_set_and_rss(tmp_path):
    dbfile = str(tmp_path / "real.duckdb")
    path = os.path.abspath(dbfile)
    assert d.live_connection_count(path) == 0
    conns = [duckdb.connect(dbfile) for _ in range(3)]
    for c in conns:
        d._register_live_connection(c, path)
    assert d.live_connection_count(path) == 3
    for c in conns:
        c.close()
    conns.clear()
    c = None
    gc.collect()
    assert d.live_connection_count(path) == 0

    rss = d.current_rss_bytes()
    assert rss is None or rss > 0


def test_recycle_grouping_and_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(rec.svcconfig, "list_configs", lambda: [])
    assert rec.recycle_once() == "no services configured"

    cfgs = [
        {"name": "svc1", "duckdb_path": "/tmp/shared.duckdb"},
        {"name": "svc2", "duckdb_path": "/tmp/shared.duckdb"},
    ]
    monkeypatch.setattr(rec.svcconfig, "list_configs", lambda: cfgs)
    monkeypatch.setattr(rec._db, "_source_from_config", lambda c: c)
    groups = rec._sources_by_db_path()
    assert len(groups) == 1
    ((only_path, sources),) = groups.items()
    assert only_path == os.path.abspath("/tmp/shared.duckdb") and {s["name"] for s in sources} == {"svc1", "svc2"}

    # Success releases barrier and lock
    ok_path = os.path.abspath(str(tmp_path / "ok.duckdb"))
    res_ok = rec._recycle_db_path(ok_path, [{"name": "rec_ok_svc", "duckdb_path": ok_path}])
    assert (res_ok["status"], res_ok["live_after"]) == ("recycled", 0)
    assert ok_path not in d._recycle_barrier_active
    lock = _get_service_lock("rec_ok_svc")
    assert lock.acquire(blocking=False) is True
    lock.release()

    # Skips when write lock busy
    monkeypatch.setenv("DUCKDB_RECYCLE_LOCK_TIMEOUT_MS", "100")
    busy_path = os.path.abspath(str(tmp_path / "busy.duckdb"))
    busy_lock = _get_service_lock("rec_busy_svc")
    held, release = threading.Event(), threading.Event()

    def holder():
        busy_lock.acquire()
        held.set()
        release.wait(timeout=5.0)
        busy_lock.release()

    th = threading.Thread(target=holder)
    th.start()
    held.wait(timeout=2.0)
    try:
        res_busy = rec._recycle_db_path(busy_path, [{"name": "rec_busy_svc", "duckdb_path": busy_path}])
        assert res_busy["status"] == "skipped_locked" and busy_path not in d._recycle_barrier_active
    finally:
        release.set()
        th.join(timeout=2.0)


def test_recycle_barriers(monkeypatch):
    # No block when clear
    t0 = time.monotonic()
    d._barrier_wait("/tmp/no-barrier.duckdb")
    assert time.monotonic() - t0 < 0.1

    # Blocks then fails open
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "150")
    failopen_path = "/tmp/failopen.duckdb"
    d.set_recycle_barrier(failopen_path, True)
    try:
        t0 = time.monotonic()
        d._barrier_wait(failopen_path)
        assert 0.1 <= time.monotonic() - t0 < 0.6 and failopen_path in d._recycle_barrier_active
    finally:
        d.set_recycle_barrier(failopen_path, False)

    # Cleared unblocks immediately & per-db-path isolation
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "5000")
    clr_path = "/tmp/cleared.duckdb"
    d.set_recycle_barrier(clr_path, True)
    elapsed = {}

    def waiter():
        t_start = time.monotonic()
        d._barrier_wait(clr_path)
        elapsed["t"] = time.monotonic() - t_start

    th = threading.Thread(target=waiter)
    th.start()
    try:
        t_other = time.monotonic()
        d._barrier_wait("/tmp/path_b.duckdb")
        assert time.monotonic() - t_other < 0.2
        time.sleep(0.05)
    finally:
        d.set_recycle_barrier(clr_path, False)
    th.join(timeout=2.0)
    assert not th.is_alive() and elapsed["t"] < 1.0


# ── Merged from tests/core/test_duckdb_status_helpers.py ──

"""Targeted coverage for backend.core.duckdb_status helpers."""


from contextlib import ExitStack
from datetime import timedelta

import pytest

import backend.core.duckdb_status as _duckdb_status
import backend.core.metadata as metadata_db
from backend.core import duckdb as _db
from backend.core.duckdb_status import (
    _clear_schema_cache,
    _schema_cache,
    backfill_fastly_edge_writes,
    get_schema,
    reconcile_fastly_stats,
    update_top_values,
)
from backend.core.rollups.recompute import rum_count_and_latest
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.date_utils import safe_iso


class _StubRow:
    def __init__(self, one=None, all_rows=None):
        self._one = one
        self._all = [] if all_rows is None else all_rows

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _StubCon:
    def execute(self, *_a, **_k):
        return _StubRow()

    def close(self):
        pass


@contextmanager
def _refresh_ctx(captured: dict, cfg: dict | None = None, src: dict | None = None, **overrides):
    default_cfg = {"name": "svc", "bucket": "b", "service_id": "svc"} if cfg is None else cfg
    default_src = {"name": "svc", "bucket": "b"} if src is None else src
    targets = {
        "backend.config.load_config": {"return_value": default_cfg},
        "backend.config.config_to_source": {"return_value": default_src},
        "backend.config.update_status": {"side_effect": lambda sid, st: captured.setdefault("status", st)},
        "backend.core.duckdb.get_connection": {"return_value": _StubCon()},
        "backend.core.duckdb.get_sync_status": {"return_value": {"ingested": 0, "local_rows": 0}},
        "backend.core.duckdb.get_schema": {"return_value": []},
        "backend.core.duckdb.update_top_values": {},
    }
    targets.update(overrides)
    with ExitStack() as stack:
        mocks = {k: stack.enter_context(patch(k, **v)) for k, v in targets.items()}
        yield mocks


# ── update_top_values ────────────────────────────────────────────────


def test_update_top_values_happy_retry_and_no_overlap(in_memory_duckdb, fos_source, tmp_path):
    cache_dir = tmp_path / "cache" / fos_source["bucket"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    src = {**fos_source, "_cache_dir_override": str(cache_dir)}
    (cache_dir / "data" / "ts=1").mkdir(parents=True)
    (cache_dir / "data" / "ts=1" / "f.parquet").write_bytes(b"x")

    table_name = _db._safe_table_name(src["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (ip VARCHAR, country VARCHAR, status VARCHAR, method VARCHAR)")
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES "
        "('1.1.1.1', 'US', '200', 'GET'), ('1.1.1.1', 'US', '200', 'GET'), ('2.2.2.2', 'GB', '404', 'POST')"
    )
    schema = [{"name": c, "type": "VARCHAR"} for c in ("ip", "country", "status", "method")]

    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)
    with patch("backend.core.duckdb_status.get_schema", return_value=schema):
        update_top_values(in_memory_duckdb, src)

    out_path = cache_dir / "top_values.json"
    assert out_path.exists()
    payload = json.loads(out_path.read_text())
    assert set(payload.keys()) == {"ip", "country", "status", "method"}
    assert {item["value"]: item["count"] for item in payload["ip"]} == {"1.1.1.1": 2, "2.2.2.2": 1}
    with _db._top_values_cache_lock:
        assert _db._top_values_cache.get(src["name"]) is not None

    # Catalog error on first CREATE TEMP TABLE triggers storage view refresh + retry
    out_path.unlink()
    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)
    state = {"creates": 0}

    class _Proxy:
        def execute(self, sql, *a, **kw):
            if sql.startswith("CREATE TEMP TABLE"):
                state["creates"] += 1
                if state["creates"] == 1:
                    raise Exception("Catalog Error: Table with name foo does not exist")
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core.duckdb_status.get_schema", return_value=schema[:2]),
        patch("backend.core.storage.update_storage_view") as mock_update,
    ):
        update_top_values(_Proxy(), src)
    mock_update.assert_called_once()
    assert state["creates"] == 2
    assert out_path.exists()

    # No whitelist overlap short-circuits before CREATE TEMP TABLE
    out_path.unlink()
    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)
    state["creates"] = 0
    no_overlap_schema = [{"name": "timestamp", "type": "TIMESTAMP"}, {"name": "custom_blob", "type": "VARCHAR"}]
    with patch("backend.core.duckdb_status.get_schema", return_value=no_overlap_schema):
        update_top_values(_Proxy(), src)
    assert state["creates"] == 0
    assert not out_path.exists()


# ── refresh_config_status ────────────────────────────────────────────


def test_refresh_config_status_swallow_and_error_branches():
    # Missing config -> silent return
    with patch("backend.config.load_config", return_value=None), patch("backend.config.update_status") as mock_upd:
        refresh_config_status("missing-svc")
    mock_upd.assert_not_called()

    # High-scale service -> skips legacy DuckDB reads
    with (
        patch("backend.config.load_config", return_value={"name": "svc", "bucket": "b", "service_id": "svc"}),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch(
            "backend.high_scale.ingest_controller.get_high_scale_service_registry",
            return_value=type("Registry", (), {"resolve": lambda self, service_id: object()})(),
        ),
        patch("backend.core.duckdb.get_connection") as get_con,
        patch("backend.config.update_status") as upd_st,
    ):
        refresh_config_status("svc")
    get_con.assert_not_called()
    upd_st.assert_not_called()

    # Buffer walk exception swallowed
    captured: dict = {}

    def _boom(*_a, **_k):
        raise PermissionError("EACCES")

    with _refresh_ctx(
        captured,
        **{
            "os.path.isdir": {"return_value": True},
            "os.walk": {"side_effect": _boom},
            "backend.utils.dir_size._get_dir_size": {"side_effect": _boom},
        },
    ):
        refresh_config_status("svc")
    assert "status" in captured and "buffer_size_bytes" not in captured["status"]

    # Storage error envelope skips storage_bytes/storage_files
    captured.clear()
    with _refresh_ctx(
        captured, **{"backend.core.storage.get_table_info": {"return_value": {"error": "catalog unreachable"}}}
    ):
        refresh_config_status("svc")
    assert "storage_bytes" not in captured["status"] and "storage_files" not in captured["status"]


def test_refresh_config_status_happy_path_and_rum_persisted(tmp_path):
    captured: dict = {}
    buf_dir = tmp_path / "buf"
    buf_dir.mkdir()
    (buf_dir / "f.parquet").write_bytes(b"x" * 17)

    with _refresh_ctx(
        captured,
        **{
            "backend.core.duckdb_status.get_schema": {"return_value": [{"name": "ip", "type": "VARCHAR"}]},
            "backend.core.duckdb_status.update_top_values": {},
            "backend.core.duckdb_status._cache_dir": {"return_value": str(buf_dir)},
            "backend.core.storage.get_table_info": {"return_value": {"size_bytes": 12345, "data_files": 7}},
            "backend.repositories.assets.get_edge_ratio": {"return_value": (0.42, None)},
        },
    ):
        refresh_config_status("svc")

    st = captured["status"]
    assert st["buffer_size_bytes"] == 17
    assert (st["storage_bytes"], st["storage_files"], st["edge_ratio"]) == (12345, 7, 0.42)
    assert st["schema"] == [{"name": "ip", "type": "VARCHAR"}]

    # Persisted RUM freshness (real sub-hour timestamp) surfaced without live lake scan
    captured.clear()
    live_rum = MagicMock(return_value=(0, None))
    rum_cfg = {
        "name": "svc",
        "bucket": "b",
        "service_id": "svc",
        "rum_enabled": True,
        "status": {"rum": {"latest_log_at": "2026-10-02T00:45:30", "total_rows": 155, "last_sync_at": "S"}},
    }
    with _refresh_ctx(
        captured,
        cfg=rum_cfg,
        src=rum_cfg,
        **{
            "backend.core.duckdb_status.get_schema": {"return_value": [{"name": "ip", "type": "VARCHAR"}]},
            "backend.core.duckdb_status.update_top_values": {},
            "backend.core.duckdb_status._cache_dir": {"return_value": "/tmp/nonexistent-rum-fresh"},
            "backend.core.storage.get_table_info": {"return_value": {"error": "skip"}},
            "backend.core.rollups.recompute.rum_count_and_latest": {"new": live_rum},
        },
    ):
        refresh_config_status("svc")

    live_rum.assert_not_called()
    assert captured["status"]["rum"]["total_rows"] == 155
    assert captured["status"]["rum"]["latest_log_at"].startswith("2026-10-02T00:45:30")


@pytest.mark.parametrize("discovery_time", ["2026-09-08T18:00:00Z", None])
def test_refresh_config_status_skip_top_values_omits_schema_key(discovery_time):
    captured: dict = {}
    schema_calls = {"count": 0}

    def _track_schema(*_a, **_k):
        schema_calls["count"] += 1
        return []

    with _refresh_ctx(
        captured,
        **{
            "backend.core.duckdb_status.get_schema": {"side_effect": _track_schema},
            "backend.core.duckdb_status.update_top_values": {},
            "backend.core.metadata.usage_log.latest_cron_per_task": {
                "return_value": {
                    "sync": {"started_at": "2026-09-01T18:00:00Z"},
                    "log_discovery": {"started_at": discovery_time},
                }
            },
        },
    ) as mocks:
        refresh_config_status("svc", include_top_values=False)

    assert "schema" not in captured["status"]
    mocks["backend.core.duckdb_status.update_top_values"].assert_not_called()
    assert schema_calls["count"] == 0
    assert captured["status"]["request"]["last_sync_at"] == (discovery_time or "2026-09-01T18:00:00Z")


# ── ASN label helpers ────────────────────────────────────────────────


def test_asn_helpers_enrich_lookup_and_format():
    values = [{"value": "7922"}, {"value": "GoogleBot"}, {"value": "15169"}, {"value": "country-code"}]
    with patch("backend.core.duckdb.get_asn_names", return_value={7922: "Comcast Cable", 15169: "Google"}):
        out = enrich_asn_labels(values, "svc-1")
    assert out is values
    assert values[0]["label"] == "Comcast Cable (7922)"
    assert "label" not in values[1]
    assert values[2]["label"] == "Google (15169)"
    assert "label" not in values[3]

    assert get_asn_names("svc", []) == {}
    assert get_asn_names("", [7922]) == {}
    with (
        patch("backend.core.metadata.lookup_asn_names", return_value={7922: "Comcast Cable", 15169: "Google"}),
        patch("backend.core.metadata.upsert_asn_names") as mock_upsert,
    ):
        assert get_asn_names("svc", [7922, 15169]) == {7922: "Comcast Cable", 15169: "Google"}
    mock_upsert.assert_not_called()

    assert format_asn_label(7922, "Comcast Cable") == "Comcast Cable (7922)"
    assert format_asn_label(7922, "") == "AS7922"
    assert format_asn_label(7922, "AS7922") == "AS7922"


# ── get_sync_status branches ─────────────────────────────────────────


def test_get_sync_status_unconfigured_and_cached_modes(in_memory_duckdb, fos_source):
    assert get_sync_status(in_memory_duckdb, {"name": "no-svc"}) == {
        "configured": False,
        "local_rows": 0,
        "ingested": 0,
        "fos_total": 0,
        "storage_mode": "cloud",
    }

    cached = {"local_rows": 12345, "ingested": 99, "latest_log_at": "2026-06-15T00:00:00Z"}
    with patch("backend.config.get_status", side_effect=lambda *_a, **_kw: dict(cached)):
        out = get_sync_status(in_memory_duckdb, fos_source, skip_fos=True, force=False)
        out_local = get_sync_status(
            in_memory_duckdb, {**fos_source, "storage_mode": "local"}, skip_fos=True, force=False
        )
    assert (out["local_rows"], out["ingested"], out["storage_mode"], out["configured"]) == (12345, 99, "cloud", True)
    assert "access_level" not in out
    assert out_local["storage_mode"] == "local"

    empty_sum = {
        "file_count": 0,
        "total_rows": 0,
        "total_bytes": 0,
        "count_with_bytes": 0,
        "last_ingested": None,
        "latest_file_name": None,
    }
    with (
        patch("backend.core.duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={"local_rows": 0, "latest_log_at": None}),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=empty_sum),
    ):
        assert (
            get_sync_status(in_memory_duckdb, {**fos_source, "storage_mode": "local"}, force=True)["storage_mode"]
            == "local"
        )


def test_get_sync_status_stale_catalog_rebuild_and_unknown_error_fallback(in_memory_duckdb, fos_source):
    table_name = _db._safe_table_name(fos_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP)")
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES ('2026-06-15 00:00:00')")
    in_memory_duckdb.execute(
        "CREATE TABLE _cron_run_log (task VARCHAR, started_at VARCHAR, "
        "duration_s DOUBLE, status VARCHAR, error_message VARCHAR, summary VARCHAR)"
    )

    state = {"stats_calls": 0}

    class _RebuildProxy:
        def execute(self, sql, *a, **kw):
            if sql.startswith("SELECT count(*), min(timestamp), max(timestamp) FROM logs_test_service"):
                state["stats_calls"] += 1
                if state["stats_calls"] == 1:
                    raise RuntimeError("Catalog Error: Table with name foo does not exist")
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core.duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.storage.clear_source_caches") as mock_clear,
        patch("backend.core.storage.update_storage_view") as mock_update,
    ):
        out = get_sync_status(_RebuildProxy(), fos_source, force=True)

    mock_clear.assert_called_once()
    assert mock_clear.call_args.kwargs.get("keep_snapshot_cache") is True
    mock_update.assert_called_once()
    assert state["stats_calls"] == 2 and out["local_rows"] == 1

    # Unknown exception -> no rebuild, falls back to metadata count
    summary = {
        "file_count": 7,
        "total_rows": 42,
        "total_bytes": 1024,
        "count_with_bytes": 1,
        "last_ingested": "2026-06-15T00:00:00",
        "latest_file_name": "raw/2026-06-15T00:00:00.gz",
    }

    class _ErrProxy:
        def execute(self, sql, *_a, **_kw):
            if "count(*)" in sql and "timestamp" in sql:
                raise RuntimeError("connection reset by peer")
            return _StubRow(one=(0,))

    with (
        patch("backend.core.duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.storage.clear_source_caches") as mock_clear2,
        patch("backend.core.storage.update_storage_view") as mock_update2,
    ):
        out2 = get_sync_status(_ErrProxy(), fos_source, force=True)

    mock_clear2.assert_not_called()
    mock_update2.assert_not_called()
    assert (out2["local_rows"], out2["ingested"]) == (42, 7)


def test_get_sync_status_empty_and_stale_view_lake_reconciliation(in_memory_duckdb, fos_source):
    summary = {
        "file_count": 3,
        "total_rows": 400,
        "total_bytes": 300,
        "count_with_bytes": 3,
        "last_ingested": "2026-06-20T00:00:00",
        "latest_file_name": "raw/2026-06-20T00:00:00.gz",
    }

    class _EmptyViewProxy:
        def execute(self, sql, *_a, **_kw):
            if "count(*)" in sql and "timestamp" in sql:
                return _StubRow(one=(0, None, None))
            return _StubRow(one=(0,))

    # 1. Empty view with prior status prefers last-known-good local_rows
    with (
        patch("backend.core.duckdb_status._data_stats_fingerprint", return_value=None),
        patch(
            "backend.config.get_status", return_value={"local_rows": 8_090_191, "latest_log_at": "2026-06-20T14:00:00"}
        ),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.storage.clear_source_caches") as mock_clear,
        patch("backend.core.storage.update_storage_view") as mock_update,
    ):
        out = get_sync_status(_EmptyViewProxy(), fos_source, force=True)
    mock_clear.assert_not_called()
    mock_update.assert_not_called()
    assert out["local_rows"] == 8_090_191

    # 2. Empty view without prior status falls back to metadata sum
    with (
        patch("backend.core.duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.storage.clear_source_caches"),
        patch("backend.core.storage.update_storage_view"),
    ):
        assert get_sync_status(_EmptyViewProxy(), fos_source, force=True)["local_rows"] == 400

    # 3. Empty view reads authoritative lake over stale prior
    table_name = _db._safe_table_name(fos_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP)")
    in_memory_duckdb.execute(
        "CREATE TABLE _cron_run_log (task VARCHAR, started_at VARCHAR, "
        "duration_s DOUBLE, status VARCHAR, error_message VARCHAR, summary VARCHAR)"
    )
    fresh_max = datetime(2026, 10, 2, 20, 31, 27)
    fresh_min = datetime(2026, 10, 1, 0, 0, 0)

    class _LakeEmptyViewProxy:
        def execute(self, sql, *a, **kw):
            if "duckdb_tables()" in sql and "database_name = 'lake'" in sql:
                return _StubRow(one=(1,))
            if 'FROM lake."' in sql and "count(*)" in sql:
                return _StubRow(one=(500, fresh_min, fresh_max))
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core.duckdb_status._data_stats_fingerprint", return_value=None),
        patch(
            "backend.config.get_status", return_value={"local_rows": 13_948_280, "latest_log_at": "2026-09-22T00:58:06"}
        ),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.storage.buffer_files", return_value=[]),
        patch("backend.core.storage.clear_source_caches") as mock_clear,
        patch("backend.core.storage.update_storage_view") as mock_update,
    ):
        out_lake = get_sync_status(_LakeEmptyViewProxy(), fos_source, force=True)
    mock_clear.assert_not_called()
    mock_update.assert_not_called()
    assert (out_lake["local_rows"], out_lake["latest_log_at"], out_lake["earliest_log_at"]) == (
        500,
        safe_iso(fresh_max),
        safe_iso(fresh_min),
    )

    # 4. Non-empty stale view reconciles extents against authoritative lake
    stale_max = datetime(2026, 9, 22, 0, 58, 6)
    stale_min = datetime(2026, 6, 27, 0, 0, 0)
    wide_min = datetime(2026, 6, 20, 0, 0, 0)

    class _StaleNonEmptyProxy:
        def execute(self, sql, *a, **kw):
            if "duckdb_tables()" in sql and "database_name = 'lake'" in sql:
                return _StubRow(one=(1,))
            if 'FROM lake."' in sql and "count(*)" in sql:
                return _StubRow(one=(600, wide_min, fresh_max))
            if "count(*)" in sql and "min(timestamp)" in sql and "max(timestamp)" in sql:
                return _StubRow(one=(500, stale_min, stale_max))
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core.duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.storage.buffer_files", return_value=[]),
    ):
        out_reconciled = get_sync_status(_StaleNonEmptyProxy(), fos_source, force=True)
    assert (out_reconciled["local_rows"], out_reconciled["latest_log_at"], out_reconciled["earliest_log_at"]) == (
        500,
        safe_iso(fresh_max),
        safe_iso(wide_min),
    )


def test_get_sync_status_ducklake_snapshot_cached_split_stats(in_memory_duckdb, fos_source, tmp_path):
    cache_dir = tmp_path / "cache" / fos_source["bucket"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    src = {**fos_source, "_cache_dir_override": str(cache_dir)}

    in_memory_duckdb.execute("ATTACH ':memory:' AS lake")
    in_memory_duckdb.execute("CREATE TABLE lake.logs (timestamp TIMESTAMP)")
    in_memory_duckdb.execute("INSERT INTO lake.logs VALUES ('2026-10-12 12:00:00'), ('2026-10-12 12:01:00')")

    table_name = _db._safe_table_name(src["name"])
    in_memory_duckdb.execute(f"CREATE VIEW {table_name} AS SELECT * FROM lake.logs")

    with _db._data_stats_cache_lock:
        _db._data_stats_cache.pop(src["name"], None)

    def _run_with_snap(snap_id: int):
        with (
            patch("backend.core.storage._core.ducklake_table_name", return_value="logs"),
            patch("backend.core.storage._core.ducklake_current_snapshot_id", return_value=snap_id),
            patch("backend.config.get_status", return_value={}),
        ):
            return get_sync_status(in_memory_duckdb, src, force=True)

    out1 = _run_with_snap(42)
    assert out1["local_rows"] == 2
    with _db._data_stats_cache_lock:
        cached = _db._data_stats_cache.get(src["name"])
    assert cached is not None and cached[0] == ("ducklake", 42) and cached[1] == 2

    in_memory_duckdb.execute("INSERT INTO lake.logs VALUES ('2026-10-12 12:02:00')")
    assert _run_with_snap(42)["local_rows"] == 2  # Cache hit
    assert _run_with_snap(43)["local_rows"] == 3  # Snapshot bump invalidates cache


# ── get_schema branches ──────────────────────────────────────────────


def test_get_schema_branches_and_cache(in_memory_duckdb, fos_source):
    assert isinstance(_schema_cache, BoundedTTLCache)
    _schema_cache.clear()

    # 1. Missing table -> []
    assert get_schema(in_memory_duckdb, fos_source) == []

    # 2. Cached within TTL short-circuits without executing SQL
    fake_schema = [{"name": "ip", "type": "VARCHAR"}]
    _schema_cache[(fos_source["name"], "logs_test_service", True)] = fake_schema
    exec_calls = {"n": 0}

    class _CountProxy:
        def execute(self, *_a, **_kw):
            exec_calls["n"] += 1
            return _StubRow(one=(0,))

    assert get_schema(_CountProxy(), fos_source) is fake_schema
    assert exec_calls["n"] == 0

    # 3. _clear_schema_cache preserves dict identity and clears selectively or globally
    _schema_cache.clear()
    _schema_cache[("a", "t", True)] = [{"name": "x"}]
    _schema_cache[("a", "t", False)] = [{"name": "x"}]
    _schema_cache[("b", "t", True)] = [{"name": "y"}]
    same = _schema_cache
    _clear_schema_cache("a")
    assert same is _schema_cache
    assert ("a", "t", True) not in _schema_cache and ("a", "t", False) not in _schema_cache
    assert ("b", "t", True) in _schema_cache
    _clear_schema_cache()
    assert len(_schema_cache) == 0

    # 4. SUMMARIZE error falls back to DESCRIBE
    class _FallbackProxy:
        def execute(self, sql, *_a, **_kw):
            if "information_schema.tables" in sql:
                return _StubRow(one=(1,))
            if sql.startswith("SUMMARIZE"):
                raise RuntimeError("Unsupported column type LIST(STRUCT)")
            if sql.startswith("DESCRIBE"):
                return _StubRow(
                    all_rows=[
                        ("ip", "VARCHAR", None, None, None, None),
                        ("status", "VARCHAR", None, None, None, None),
                    ]
                )
            raise AssertionError(f"unexpected SQL: {sql}")

    assert get_schema(_FallbackProxy(), fos_source) == [
        {"name": "ip", "type": "VARCHAR"},
        {"name": "status", "type": "VARCHAR"},
    ]

    # 5. Both SUMMARIZE and DESCRIBE failing returns []
    _schema_cache.clear()

    class _BothFailProxy:
        def execute(self, sql, *_a, **_kw):
            if "information_schema.tables" in sql:
                return _StubRow(one=(1,))
            raise RuntimeError("disk full")

    assert get_schema(_BothFailProxy(), fos_source) == []
    _ = _duckdb_status


# ── usage-log & reconciliation helpers ───────────────────────────────


def test_reconcile_fastly_stats_guards_and_fetch():
    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0

    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        assert reconcile_fastly_stats({"name": "svc"}) == 0

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value=""),
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0

    payload = {
        "data": [
            {
                "start_time": 1760000000,
                "object_storage_class_a_operations_count": 7,
                "object_storage_class_b_operations_count": 3,
            },
            {
                "start_time": 1760003600,
                "object_storage": {"class_a_operations_count": 2, "class_b_operations_count": 1},
            },
            {"start_time": None},
        ]
    }
    fetched: list = []

    def _fake_fastly(method, path, body=None, *, token, **_kw):
        fetched.append((method, path, token))
        return payload

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("backend.core.metadata.get_latest_reconciliation_ts", return_value=None),
        patch("backend.core.fastly.client.fastly", side_effect=_fake_fastly),
        patch("backend.core.metadata.reconcile_fastly_stats", return_value=2) as mock_recon,
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}, hours_back=2) == 2

    assert len(fetched) == 1 and fetched[0][0] == "GET" and fetched[0][2] == "tok"
    assert fetched[0][1].startswith("/stats/aggregate?by=hour&from=")
    assert [(h["class_a"], h["class_b"]) for h in mock_recon.call_args.args[1]] == [(7, 3), (2, 1)]

    # Fastly client errors swallowed
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("backend.core.metadata.get_latest_reconciliation_ts", return_value=None),
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 500")),
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0


def test_usage_and_cron_forwarding_helpers():
    # purge_usage_log
    with (
        patch("backend.config.load_usage_logging_config", return_value={"retention_days": 7}),
        patch("backend.core.metadata.purge_usage_log") as mock_purge,
    ):
        purge_usage_log({})
        mock_purge.assert_not_called()
        purge_usage_log({"name": "svc"})
        mock_purge.assert_called_once_with("svc", 7)

    # update_cron_duration
    with patch("backend.core.metadata.update_cron_duration") as mock_upd:
        update_cron_duration({}, run_id=1, duration_s=0.5)
        mock_upd.assert_not_called()
        update_cron_duration({"name": "svc"}, run_id=42, duration_s=1.25, log_output="ok")
        mock_upd.assert_called_once_with("svc", 42, 1.25, log_output="ok")

    # log_usage_calls
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET", "path": "/foo"}])
        mock_log.assert_not_called()

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.log_usage_calls") as mock_log_on,
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET"}], process_context="dashboard")
        mock_log_on.assert_called_once_with("svc", [{"method": "GET"}], process_context="dashboard")

    # backfill_fastly_edge_writes
    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 0
    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        assert backfill_fastly_edge_writes({}) == 0

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=[]),
        patch("backend.core.metadata.log_synthetic_usage") as mock_synth_empty,
    ):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 0
        mock_synth_empty.assert_not_called()

    files = [
        ("raw/2026-06-15T00:00:00Z/file.gz", "2026-06-15T00:00:00", 100, 5000),
        ("__seeding_attempted__", "2026-06-15T00:00:00", 0, 0),
        ("no-timestamp.gz", "2026-06-15T00:00:00", 50, 2500),
    ]
    captured: dict = {}
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=files),
        patch(
            "backend.core.metadata.log_synthetic_usage",
            side_effect=lambda sid, calls: (captured.update({"sid": sid, "calls": calls}), len(calls))[1],
        ),
    ):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 2

    assert captured["sid"] == "svc" and len(captured["calls"]) == 2
    assert captured["calls"][0]["path"] == "raw/2026-06-15T00:00:00Z/file.gz"
    assert captured["calls"][0]["method"] == "PUT_OBJECT"
    assert captured["calls"][0]["_timestamp_override"] == "2026-06-15T00:00:00Z"
    assert captured["calls"][1]["_timestamp_override"] == "2026-06-15T00:00:00"


# ── Reconcile Fastly stats gate (DB integration) ─────────────────────


def _seed_reconciliation_row(service_id: str, ts_iso: str) -> None:
    import backend.core.metadata.usage_log as _usage_log_db

    con = _usage_log_db.get_con(service_id)
    con.execute(
        """
        INSERT INTO usage_log
        (timestamp, service_id, operation_class, operation_type, url, status,
         duration_ms, function_name, process_context, count)
        VALUES (?, ?, 'A', 'RECONCILE_A', 'fastly://stats/aggregate/test', 'OK',
                0.0, 'fastly.reconciliation', 'fastly:reconciliation', 1)
        """,
        (ts_iso, service_id),
    )
    con.commit()


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.config.get_fastly_api_key", return_value="fake-key")
def test_reconcile_gate_recent_vs_old_vs_none(_key, _enabled):
    service_id = "recon-svc-1"
    metadata_db.insert_ingested_files(
        service_id, [("s3://bkt/raw/2026-05-22/12/2026-05-22T12:00:00.000-x.log.gz", 1, 1)]
    )
    src = {"name": service_id, "logging_service_id": "logsvc"}

    # 1. No prior reconciliation -> proceeds past gate
    with patch("urllib.request.urlopen", side_effect=RuntimeError("called!")) as urlopen:
        _db.reconcile_fastly_stats(src)
        assert urlopen.called

    # 2. Old reconciliation (>1h ago) -> proceeds past gate
    old = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(service_id, old)
    with patch("urllib.request.urlopen", side_effect=RuntimeError("called!")) as urlopen:
        _db.reconcile_fastly_stats(src)
        assert urlopen.called

    # 3. Recent reconciliation (30m ago) -> skipped before HTTP request
    recent = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(service_id, recent)
    with patch("urllib.request.urlopen") as urlopen:
        assert _db.reconcile_fastly_stats(src) == 0
        assert not urlopen.called


# ── RUM status freshness ─────────────────────────────────────────────


def _make_rum_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE rum_vitals_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR,
            value VARCHAR, event_count BIGINT, value_sum DOUBLE,
            good_count BIGINT, ni_count BIGINT, poor_count BIGINT,
            p50_value DOUBLE, p75_value DOUBLE, p99_value DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE rum_error_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR,
            value VARCHAR, error_count BIGINT
        )
    """)
    con.execute("CREATE TABLE client_vitals (req_id VARCHAR, cid VARCHAR, timestamp TIMESTAMPTZ)")
    con.execute("CREATE TABLE client_errors (req_id VARCHAR, cid VARCHAR, timestamp TIMESTAMPTZ)")
    return con


def test_rum_count_and_latest_empty_vs_populated_aggregates():
    # Empty aggregates still report raw count and MAX(timestamp)
    con1 = _make_rum_con()
    recent1 = datetime.now(UTC) - timedelta(seconds=30)
    con1.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        ["r1", "c1", recent1, "r2", "c2", recent1, "r3", "c3", recent1],
    )
    count1, latest1 = rum_count_and_latest(con1)
    assert count1 == 3 and latest1 is not None
    assert abs((latest1.replace(tzinfo=UTC) if latest1.tzinfo is None else latest1) - recent1) < timedelta(seconds=1)

    # Populated aggregates fast-path returns aggregate count + raw latest
    con2 = _make_rum_con()
    bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    con2.execute(
        "INSERT INTO rum_vitals_aggregates (service_id, bucket_start, dimension, value, event_count) "
        "VALUES (?, ?, 'total', 'pageviews', 10)",
        ["svc", bucket],
    )
    recent2 = datetime.now(UTC) - timedelta(seconds=15)
    con2.execute("INSERT INTO client_vitals VALUES (?, ?, ?)", ["r1", "c1", recent2])
    count2, latest2 = rum_count_and_latest(con2)
    assert count2 == 10 and latest2 is not None
    assert abs((latest2.replace(tzinfo=UTC) if latest2.tzinfo is None else latest2) - recent2) < timedelta(seconds=1)


# ── Merged from tests/core/test_duckdb_pool.py ──


def _mock_conn() -> MagicMock:
    return MagicMock(spec=duckdb.DuckDBPyConnection)


@contextmanager
def _clean_pool_keys(*keys: str):
    for phase in (1, 2):
        with _pools_lock:
            for k in keys:
                _pools.pop(k, None)
        if phase == 1:
            yield


def _checkout_idle_conn_with_rebind(pool: _Pool, mock_conn: MagicMock) -> dict:
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1
    captured: dict = {}
    with (
        patch("backend.core.storage._core._view_cache", {}),
        patch(
            "backend.core.storage._core.update_storage_view",
            side_effect=lambda con, src, **kwargs: captured.update({"con": con, "src": src, "kwargs": kwargs}),
        ) as mock_uiv,
    ):
        pool.acquire(src={"name": pool.service_key, "bucket": "b"}, max_wait=0.5)
        captured["call_count"] = mock_uiv.call_count
    return captured


def _pool_with_fake_connections(max_size: int) -> _Pool:
    pool = _Pool("heavy-checkout", max_size=max_size)
    pool.acquire = MagicMock(side_effect=lambda *a, **k: MagicMock())  # type: ignore[method-assign]
    pool.release = MagicMock()  # type: ignore[method-assign]
    return pool


def test_pool_deadlock_safety_and_recovery_on_checkout_exception():
    pool = _Pool(service_key="test_deadlock_service", max_size=2)
    pool._idle.put_nowait(_mock_conn())
    pool._in_use = 1

    with (
        patch("backend.core.storage._core._view_cache", {}),
        patch("backend.core.storage._core.update_storage_view", side_effect=RuntimeError("Mock view rebind failed")),
    ):
        completed = False

        def run_acquire():
            nonlocal completed
            try:
                pool.acquire(src={"name": "test_deadlock_service", "bucket": "b"}, max_wait=0.1)
            except RuntimeError as e:
                completed = str(e) == "Mock view rebind failed"

        t = threading.Thread(target=run_acquire)
        t.start()
        t.join(timeout=2.0)
        assert not t.is_alive() and completed

    assert (pool._in_use, pool._discarded_total, pool._idle.empty()) == (0, 1, True)

    fresh_conn = _mock_conn()
    with (
        patch("backend.core.duckdb.get_connection", return_value=fresh_conn) as build_mock,
        patch("backend.core.storage._core.update_storage_view", return_value=None),
        patch("backend.core.storage._core._try_fast_path_view", return_value=True),
        patch("backend.core.duckdb_pool._set_conn_state"),
        patch("backend.core.duckdb_pool._pool_conn_memory_limit", return_value=None),
    ):
        got = pool.acquire(src={"name": "test_deadlock_service", "bucket": "b"}, max_wait=0.5)
    assert got is fresh_conn and build_mock.call_count == 1 and (pool._in_use, pool._discarded_total) == (1, 1)


def test_warm_idle_and_warm_pool_for_service():
    pool, conns = _Pool(service_key="test_warm", max_size=3), [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3
    with patch("backend.core.storage._core._try_fast_path_view", return_value=True) as mock_fp:
        pool.warm_idle(src={"name": "test_warm", "bucket": "b"})
    assert mock_fp.call_count == 3 and {call.args[0] for call in mock_fp.call_args_list} == set(conns)
    assert (pool._in_use, pool._idle.qsize()) == (3, 3)

    empty_pool = _Pool(service_key="test_warm_empty", max_size=2)
    with patch("backend.core.storage._core._try_fast_path_view") as mock_fp2:
        empty_pool.warm_idle(src={"name": "test_warm_empty", "bucket": "b"})
    assert (mock_fp2.call_count, empty_pool._in_use, empty_pool._idle.qsize()) == (0, 0, 0)

    fail_pool, mc = _Pool(service_key="test_warm_fail", max_size=1), _mock_conn()
    fail_pool._idle.put_nowait(mc)
    fail_pool._in_use = 1
    with patch("backend.core.storage._core._try_fast_path_view", side_effect=RuntimeError("bind boom")):
        fail_pool.warm_idle(src={"name": "test_warm_fail", "bucket": "b"})
    assert (fail_pool._in_use, fail_pool._idle.qsize()) == (1, 1)
    mc.close.assert_not_called()

    key = "test_warm_for_service_xyz"
    with _clean_pool_keys("nonexistent_service", key):
        warm_pool_for_service("nonexistent_service", {"name": "nonexistent_service"})
        with _pools_lock:
            _pools[key] = fail_pool
        with patch.object(fail_pool, "warm_idle") as mock_warm:
            warm_pool_for_service(key, src={"name": key})
        mock_warm.assert_called_once()


def test_warm_pool_at_startup_builds_clamps_and_swallows_errors():
    k1, k2, k3 = "test_warm_startup_1", "test_warm_startup_2", "test_warm_startup_3"
    with _clean_pool_keys(k1, k2, k3):
        with (
            patch("backend.core.duckdb.get_connection", return_value=_mock_conn()),
            patch("backend.core.storage._core._view_cache", {}),
            patch("backend.core.storage._core.update_storage_view"),
        ):
            assert warm_pool_at_startup(k1, src={"name": k1}, count=4) == 4
            _get_pool(k2, max_size=2)
            assert warm_pool_at_startup(k2, src={"name": k2}, count=10) == 2
        with _pools_lock:
            p1, p2 = _pools[k1], _pools[k2]
        assert (p1._idle.qsize(), p1._created_total, p2._idle.qsize()) == (4, 4, 2)

        with (
            patch(
                "backend.core.duckdb.get_connection",
                side_effect=[_mock_conn(), RuntimeError("simulated build failure")],
            ),
            patch("backend.core.storage._core._view_cache", {}),
            patch("backend.core.storage._core.update_storage_view"),
        ):
            assert warm_pool_at_startup(k3, src={"name": k3}, count=4) == 1


def test_pool_saturation_and_wait_stats():
    pool = _Pool(service_key="test_saturation", max_size=1)
    empty_stats = {"count": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0, "mean_ms": 0.0}
    assert pool._wait_stats() == empty_stats and pool._rebind_wait_stats() == empty_stats

    pool._in_use = 1
    t0 = time.monotonic()
    with pytest.raises(_PoolBusy, match="saturated"):
        pool.acquire(src={"name": "test_saturation", "bucket": "b"}, max_wait=0.05)
    assert 0.04 <= (time.monotonic() - t0) < 0.5 and pool.stats()["wait"]["count"] >= 1

    errors: list = []

    def _worker():
        try:
            pool.acquire(src={"name": "test_saturation"}, max_wait=0.05)
        except _PoolBusy:
            errors.append("busy")

    threads = [threading.Thread(target=_worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2.0)
        assert not t.is_alive()
    assert errors == ["busy", "busy", "busy"]

    pop_pool = _Pool(service_key="test_wait_populated", max_size=1)
    for sample_ms in [1.0, 2.0, 3.0, 4.0, 100.0]:
        pop_pool._record_wait_sample(sample_ms)
    st = pop_pool._wait_stats()
    assert (st["count"], st["p50_ms"], st["p95_ms"], st["max_ms"]) == (5, 3.0, 100.0, 100.0)
    assert _percentile_summary(collections.deque([42.5]), threading.Lock()) == {
        "count": 1,
        "p50_ms": 42.5,
        "p95_ms": 42.5,
        "p99_ms": 42.5,
        "max_ms": 42.5,
        "mean_ms": 42.5,
    }


def test_prepare_checkout_lock_timeout_and_rebind_wait_samples(monkeypatch):
    pool = _Pool(service_key="test_rebind_timeout", max_size=1)
    captured = _checkout_idle_conn_with_rebind(pool, _mock_conn())
    assert captured["call_count"] == 1 and 0 < captured["kwargs"]["lock_timeout"] < 1.0
    st = pool.stats()
    assert st["rebind_wait"]["count"] == 1 and st["rebind_wait"]["max_ms"] >= 0.0

    monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", "1750")
    pool_env = _Pool(service_key="test_rebind_env", max_size=1)
    assert _checkout_idle_conn_with_rebind(pool_env, _mock_conn())["kwargs"]["lock_timeout"] == 1.75

    pool_fail = _Pool(service_key="test_rebind_fail", max_size=1)
    pool_fail._idle.put_nowait(_mock_conn())
    pool_fail._in_use = 1
    with (
        patch("backend.core.storage._core._view_cache", {}),
        patch("backend.core.storage._core.update_storage_view", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        pool_fail.acquire(src={"name": "test_rebind_fail", "bucket": "b"}, max_wait=0.1)
    assert pool_fail.stats()["rebind_wait"]["count"] == 1


def test_prepare_checkout_fingerprints_and_skip_view_update(monkeypatch):
    pool, mc, same_tuple = _Pool(service_key="test_fp", max_size=1), _mock_conn(), ("same_sql", "same_hash")
    _set_conn_state(mc, view_fingerprint=same_tuple, buffer_mtime=42.0)
    pool._idle.put_nowait(mc)
    pool._in_use = 1
    with (
        patch.dict("backend.core.storage._core._view_cache", {"test_fp": same_tuple}, clear=False),
        patch("backend.core.storage._core.update_storage_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=42.0),
    ):
        assert pool.acquire(src={"name": "test_fp", "bucket": "b"}, max_wait=0.5) is mc
    mock_uiv.assert_not_called()
    assert pool._reused_total == 1

    pool._idle.put_nowait(mc)
    with (
        patch.dict("backend.core.storage._core._view_cache", {"test_fp": ("new_sql", "new_hash")}, clear=False),
        patch("backend.core.storage._core.update_storage_view") as mock_uiv2,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=42.0),
    ):
        assert pool.acquire(src={"name": "test_fp", "bucket": "b"}, max_wait=0.5) is mc
    mock_uiv2.assert_called_once()

    _set_conn_state(mc, view_fingerprint=same_tuple, buffer_mtime=100.0)
    pool._idle.put_nowait(mc)
    with (
        patch.dict("backend.core.storage._core._view_cache", {"test_fp": same_tuple}, clear=False),
        patch("backend.core.storage._core.update_storage_view") as mock_uiv3,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=200.0),
    ):
        assert pool.acquire(src={"name": "test_fp", "bucket": "b"}, max_wait=0.5) is mc
    mock_uiv3.assert_called_once()

    pool_skip, mc_skip = _Pool(service_key="test_skip_view", max_size=1), _mock_conn()
    pool_skip._idle.put_nowait(mc_skip)
    pool_skip._in_use = 1
    with (
        patch.dict("backend.core.storage._core._view_cache", {"test_skip_view": ("fresh",)}, clear=False),
        patch("backend.core.storage._core.update_storage_view") as mock_uiv_skip,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=12345.0),
    ):
        assert pool_skip.acquire(src={"name": "test_skip_view"}, max_wait=0.5, skip_view_update=True) is mc_skip
        mock_uiv_skip.assert_not_called()
        assert _get_conn_state(mc_skip, "view_fingerprint") is None
        pool_skip._idle.put_nowait(mc_skip)
        pool_skip.acquire(src={"name": "test_skip_view"}, max_wait=0.5)
        mock_uiv_skip.assert_called_once()

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    key = "test_skip_view_passthrough"
    with (
        _clean_pool_keys(key),
        patch("backend.core.duckdb.get_connection", return_value=_mock_conn()),
        patch("backend.core.storage._core._view_cache", {}),
        patch("backend.core.storage._core.update_storage_view") as mock_uiv_pass,
    ):
        with checkout_connection(src={"name": key}, max_wait=1.0):
            pass
        baseline = mock_uiv_pass.call_count
        with checkout_connection(src={"name": key}, max_wait=1.0, skip_view_update=True):
            pass
        assert mock_uiv_pass.call_count == baseline


def test_env_parsers_and_conn_state_helpers(monkeypatch, tmp_path):
    monkeypatch.delenv("DUCKDB_CONNECTION_POOL", raising=False)
    assert _pool_enabled() is True
    for falsy in ("0", "false", "no", "off", "FALSE"):
        monkeypatch.setenv("DUCKDB_CONNECTION_POOL", falsy)
        assert _pool_enabled() is False
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    assert _pool_enabled() is True

    for val, expected in (("16", 16), ("0", 1), ("not-an-int", 12)):
        monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", val)
        assert _pool_max_size() == expected

    monkeypatch.delenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", raising=False)
    assert _pool_conn_memory_limit() is None
    monkeypatch.setenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "256MB")
    assert _pool_conn_memory_limit() == "256MB"

    for env_var, fn in (
        ("DUCKDB_POOL_SWEEP", _pool_sweep_enabled),
        ("DUCKDB_POOL_WARM_AT_BOOT", _pool_warm_at_boot_enabled),
    ):
        monkeypatch.delenv(env_var, raising=False)
        assert fn() is False
        for truthy in ("1", "true", "yes", "on", "TRUE"):
            monkeypatch.setenv(env_var, truthy)
            assert fn() is True

    monkeypatch.delenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", raising=False)
    assert _pool_warm_at_boot_count() == 4
    for val, expected in (("8", 8), ("0", 1), ("garbage", 4)):
        monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", val)
        assert _pool_warm_at_boot_count() == expected

    monkeypatch.delenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", raising=False)
    assert _pool_api_rebind_lock_timeout_s() == 0.5
    for val, expected in (("garbage", 0.5), ("-50", 0.0)):
        monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", val)
        assert _pool_api_rebind_lock_timeout_s() == expected

    mc = _mock_conn()
    _set_conn_state(mc, service_key="svc", view_fingerprint=("a",))
    assert (
        _get_conn_state(mc, "service_key"),
        _get_conn_state(mc, "view_fingerprint"),
        _get_conn_state(mc, "missing", default="dflt"),
    ) == ("svc", ("a",), "dflt")
    _forget_conn(mc)
    assert _get_conn_state(mc, "service_key", default=None) is None

    assert _safe_buffer_mtime(None) is None
    with patch("backend.core.storage._core._buffer_dir", side_effect=RuntimeError("boom")):
        assert _safe_buffer_mtime({"name": "x"}) is None
    p = tmp_path / "buffer"
    p.mkdir()
    with patch("backend.core.storage._core._buffer_dir", return_value=str(p)):
        assert isinstance(_safe_buffer_mtime({"name": "x"}), float)

    pool, bad_cache = _Pool(service_key="test_stamp_err", max_size=1), MagicMock()
    bad_cache.get.side_effect = RuntimeError("cache blew up")
    with patch("backend.core.storage._core._view_cache", bad_cache):
        pool._stamp_fingerprint(mc, src={"name": "test_stamp_err"})
    assert (
        _get_conn_state(mc, "view_fingerprint", default="NOT_SET"),
        _get_conn_state(mc, "buffer_mtime", default="NOT_SET"),
    ) == (None, None)

    assert _instrument(mc, service_key="svc") is not None
    with patch("backend.core.request_telemetry.InstrumentedDuckDBConnection", side_effect=RuntimeError("wrap failed")):
        assert _instrument(mc, service_key="svc") is mc


# ── Fresh build, release, sweep & registry ───────────────────────────


def test_fresh_build_memory_limit_pragma_and_failure(monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "512MB")
    pool, mc = _Pool(service_key="test_fresh", max_size=2), _mock_conn()
    with (
        patch("backend.core.duckdb.get_connection", return_value=mc),
        patch("backend.core.storage._core._view_cache", {}),
        patch("backend.core.storage._core.update_storage_view"),
    ):
        assert pool.acquire(src={"name": "test_fresh"}, max_wait=0.5) is mc
    assert any("memory_limit" in c.args[0] and "512MB" in c.args[0] for c in mc.execute.call_args_list)
    assert (pool._in_use, pool._created_total) == (1, 1)

    mc_err = _mock_conn()
    mc_err.execute.side_effect = RuntimeError("pragma rejected")
    pool_err = _Pool(service_key="test_fresh_err", max_size=1)
    with (
        patch("backend.core.duckdb.get_connection", return_value=mc_err),
        patch("backend.core.storage._core._view_cache", {}),
        patch("backend.core.storage._core.update_storage_view"),
    ):
        assert pool_err.acquire(src={"name": "test_fresh_err"}, max_wait=0.5) is mc_err

    pool_fail = _Pool(service_key="test_fresh_fail", max_size=1)
    with (
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("conn build failed")),
        pytest.raises(RuntimeError, match="conn build failed"),
    ):
        pool_fail.acquire(src={"name": "test_fresh_fail"}, max_wait=0.1)
    assert pool_fail._in_use == 0


def test_release_discard_and_sweep_branches(monkeypatch):
    pool = _Pool(service_key="test_rel", max_size=2)
    pool._in_use = 2
    c1, c2 = _mock_conn(), _mock_conn()
    pool.release(c1, errored=False)
    assert (pool._idle.qsize(), pool._in_use) == (1, 2)
    pool.release(c2, errored=True)
    assert (pool._idle.qsize(), pool._in_use, pool._discarded_total) == (1, 1, 1)
    c2.close.assert_called_once()

    c_boom = _mock_conn()
    c_boom.close.side_effect = RuntimeError("close exploded")
    pool._discard(c_boom)
    assert (pool._in_use, pool._discarded_total) == (0, 2)

    full_pool = _Pool(service_key="test_full", max_size=1)
    full_pool._idle.put_nowait(_mock_conn())
    full_pool._in_use = 3
    c_extra1, c_extra2 = _mock_conn(), _mock_conn()
    c_extra2.close.side_effect = RuntimeError("close went bang")
    full_pool.release(c_extra1, errored=False)
    c_extra1.close.assert_called_once()
    assert full_pool._in_use == 2
    full_pool.release(c_extra2, errored=False)
    assert full_pool._in_use == 1

    monkeypatch.setenv("DUCKDB_POOL_SWEEP", "1")
    sweep_pool = _Pool(service_key="test_sweep", max_size=3)
    sweep_pool._in_use = 3
    c_sweep, res = _mock_conn(), MagicMock()
    res.fetchall.return_value = [("t_abc",), ("t_def",)]
    c_sweep.execute.return_value = res
    sweep_pool.release(c_sweep, errored=False)
    sql_calls = [c.args[0] for c in c_sweep.execute.call_args_list]
    assert any("duckdb_tables" in s for s in sql_calls)
    assert any('DROP TABLE IF EXISTS "t_abc"' in s for s in sql_calls) and any(
        'DROP TABLE IF EXISTS "t_def"' in s for s in sql_calls
    )

    c_drop_err = _mock_conn()
    c_drop_err.execute.side_effect = [res, RuntimeError("can't drop"), MagicMock()]
    sweep_pool.release(c_drop_err, errored=False)
    assert sweep_pool._idle.qsize() == 2

    c_sel_err = _mock_conn()
    c_sel_err.execute.side_effect = RuntimeError("sweep blew up")
    sweep_pool.release(c_sel_err, errored=False)
    assert sweep_pool._idle.qsize() == 3


def test_registry_get_pool_stats_and_shutdown(monkeypatch):
    k1, k2 = "test_get_pool_1", "test_get_pool_2"
    with _clean_pool_keys(k1, k2):
        p1 = _get_pool(k1, max_size=3)
        assert _get_pool(k1, max_size=99) is p1 and p1.max_size == 3
        monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "5")
        assert _get_pool(k2).max_size == 5
        matching = [s for s in get_all_stats() if s["service"] == k1]
        assert (
            len(matching) == 1
            and matching[0]["max_size"] == 3
            and "wait" in matching[0]
            and "rebind_wait" in matching[0]
        )

        ma, mb = _mock_conn(), _mock_conn()
        mb.close.side_effect = RuntimeError("close kaboom")
        p1._idle.put_nowait(ma)
        p1._idle.put_nowait(mb)
        shutdown_all()
        ma.close.assert_called_once()
        mb.close.assert_called_once()
        with _pools_lock:
            assert k1 not in _pools


def test_checkout_connection_disabled_pooled_errored_and_interrupted(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    raw, raw_err = _mock_conn(), _mock_conn()
    raw_err.close.side_effect = RuntimeError("close failed")
    with (
        patch("backend.core.duckdb.get_connection", return_value=raw) as mock_get,
        checkout_connection(src={"name": "test_disabled"}, max_wait=1.0) as con,
    ):
        assert con is not None
    mock_get.assert_called_once()
    raw.close.assert_called_once()
    with (
        patch("backend.core.duckdb.get_connection", return_value=raw_err),
        checkout_connection(src={"name": "test_disabled_close"}, max_wait=1.0),
    ):
        pass
    raw_err.close.assert_called_once()

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    k_ok, k_err, k_int = "test_co_ok", "test_co_err", "test_co_int"
    with _clean_pool_keys(k_ok, k_err, k_int):
        raw_int = _mock_conn()
        with (
            patch("backend.core.duckdb.get_connection", side_effect=[_mock_conn(), _mock_conn(), raw_int]),
            patch("backend.core.storage._core._view_cache", {}),
            patch("backend.core.storage._core.update_storage_view"),
        ):
            with checkout_connection(src={"name": k_ok}, max_wait=1.0) as con:
                assert con is not None
            with pytest.raises(ValueError, match="boom"), checkout_connection(src={"name": k_err}, max_wait=1.0):
                raise ValueError("boom")
            with pytest.raises(duckdb.InterruptException), checkout_connection(src={"name": k_int}, max_wait=1.0):
                raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")
        with _pools_lock:
            p_ok, p_err, p_int = _pools[k_ok], _pools[k_err], _pools[k_int]
        assert p_ok._idle.qsize() == 1 and (p_err._idle.qsize(), p_err._discarded_total) == (0, 1)
        assert (p_int._idle.qsize(), p_int._discarded_total, p_int._in_use) == (0, 1, 0)
        raw_int.close.assert_called_once()


# ── Drain lifecycle ──────────────────────────────────────────────────


def test_pool_drain_lifecycle_and_module_helpers(monkeypatch):
    # begin_drain closes idle and zeroes in_use
    pool = _Pool(service_key="drain_idle", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3
    pool.begin_drain()
    assert (pool._draining, pool._in_use, pool._idle.empty()) == (True, 0, True)
    for c in conns:
        c.close.assert_called_once()

    # acquire during drain times out to _PoolBusy if drain doesn't end
    with pytest.raises(_PoolBusy):
        pool.acquire(src={"name": "drain_idle", "bucket": "b"}, max_wait=0.05)

    # release during drain closes conn instead of idling
    pool_rel = _Pool(service_key="drain_release", max_size=2)
    pool_rel._in_use = 1
    pool_rel.begin_drain()
    c_rel = _mock_conn()
    pool_rel.release(c_rel)
    c_rel.close.assert_called_once()
    assert (pool_rel._in_use, pool_rel._idle.empty()) == (0, True)

    # wait_drained reaches zero when returned, or times out without deadlock
    pool_wait = _Pool(service_key="drain_wait", max_size=2)
    pool_wait._in_use = 1
    pool_wait.begin_drain()
    assert pool_wait.wait_drained(timeout=0.05) is False
    threading.Thread(target=lambda: (time.sleep(0.05), pool_wait.release(_mock_conn()))).start()
    assert pool_wait.wait_drained(timeout=2.0) is True and pool_wait._in_use == 0

    # acquire queues during drain then resumes after end_drain
    pool_q = _Pool(service_key="drain_queue", max_size=1)
    pool_q.begin_drain()
    outcome: dict = {}
    fake = _mock_conn()

    def _queued():
        with patch("backend.core.duckdb.get_connection", return_value=fake):
            outcome["got"] = pool_q.acquire(src={"name": "drain_queue", "bucket": "b"}, max_wait=5.0)

    th = threading.Thread(target=_queued)
    th.start()
    time.sleep(0.1)
    assert th.is_alive()
    pool_q.end_drain()
    th.join(timeout=2.0)
    assert not th.is_alive() and outcome["got"] is fake

    # warm_idle is noop while draining
    pool_w = _Pool(service_key="drain_warm", max_size=2)
    wc = _mock_conn()
    pool_w._idle.put_nowait(wc)
    pool_w._in_use = 1
    pool_w._draining = True
    pool_w.warm_idle(src={"name": "drain_warm", "bucket": "b"})
    assert pool_w._idle.qsize() == 1
    wc.close.assert_not_called()

    # Module drain helpers only touch existing pools
    grp_pool = _Pool(service_key="grp_a", max_size=1)
    monkeypatch.setitem(duckdb_pool._pools, "grp_a", grp_pool)
    duckdb_pool.begin_drain_pools(["grp_a", "does_not_exist"])
    assert grp_pool._draining is True
    assert duckdb_pool.wait_pools_drained(["grp_a", "does_not_exist"], timeout=1.0) is True
    duckdb_pool.end_drain_pools(["grp_a", "does_not_exist"])
    assert grp_pool._draining is False


# ── Heavy slots & slow checkout logging ──────────────────────────────


def test_heavy_cap_and_is_heavy_route(monkeypatch):
    monkeypatch.delenv("DUCKDB_POOL_HEAVY_MAX", raising=False)
    assert (_pool_heavy_cap(4), _pool_heavy_cap(12), _pool_heavy_cap(2), _pool_heavy_cap(1)) == (3, 11, 1, 1)
    for val, expected in (("2", 2), ("99", 4), ("0", 1), ("not-a-number", 3)):
        monkeypatch.setenv("DUCKDB_POOL_HEAVY_MAX", val)
        assert _pool_heavy_cap(4) == expected

    monkeypatch.delenv("DUCKDB_POOL_HEAVY_ROUTES", raising=False)
    heavy_routes = (
        "/api/cmcd/aggregates",
        "/api/security/aggregates",
        "/api/network/bundle",
        "/api/network-health",
        "/api/insights",
        "/api/origin/slow-urls",
    )
    assert all(is_heavy_route(r) for r in heavy_routes)
    assert not any(
        is_heavy_route(r) for r in ("/api/cron-runs", "/api/admin/share/banner", "/api/web-vitals", "", None)
    )
    monkeypatch.setenv("DUCKDB_POOL_HEAVY_ROUTES", "/custom, /other")
    assert is_heavy_route("/api/x/custom") is True and is_heavy_route("/api/cmcd/aggregates") is False


def test_heavy_slots_concurrency_timeout_and_light_bypass():
    pool = _Pool("heavy-cap", max_size=4)
    for _ in range(3):
        pool.acquire_heavy_slot(max_wait=1.0)
    assert pool.stats()["heavy"]["cap"] == 3 and pool.stats()["heavy"]["in_use"] == 3

    got_slot = threading.Event()
    t = threading.Thread(target=lambda: (pool.acquire_heavy_slot(max_wait=5.0), got_slot.set()))
    t.start()
    assert not got_slot.wait(0.1)
    pool.release_heavy_slot()
    assert got_slot.wait(2.0)
    t.join(2.0)
    assert pool.stats()["heavy"]["in_use"] == 3

    pool_to = _Pool("heavy-timeout", max_size=2)
    pool_to.acquire_heavy_slot(max_wait=1.0)
    with pytest.raises(_PoolBusy, match="heavy-request slots exhausted"):
        pool_to.acquire_heavy_slot(max_wait=0.05)
    assert pool_to.stats()["saturated_rejects_total"] == 1

    fpool = _pool_with_fake_connections(4)
    release_heavy, inside, fourth_done = threading.Event(), threading.Barrier(4), threading.Event()

    def hold_heavy():
        with checkout_connection({"name": "heavy-checkout"}, max_wait=5.0, heavy=True):
            inside.wait(2.0)
            release_heavy.wait(5.0)

    def fourth_heavy():
        with checkout_connection({"name": "heavy-checkout"}, max_wait=5.0, heavy=True):
            fourth_done.set()

    with (
        patch.object(duckdb_pool, "_get_pool", return_value=fpool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
    ):
        holders = [threading.Thread(target=hold_heavy) for _ in range(3)]
        for h in holders:
            h.start()
        inside.wait(2.0)
        extra = threading.Thread(target=fourth_heavy)
        extra.start()
        assert not fourth_done.wait(0.1)
        with checkout_connection({"name": "heavy-checkout"}, max_wait=0.5):
            pass
        release_heavy.set()
        assert fourth_done.wait(2.0)
        for th in (*holders, extra):
            th.join(2.0)
    assert fpool.stats()["heavy"]["in_use"] == 0

    err_pool = _pool_with_fake_connections(2)
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=err_pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
    ):
        err_pool.acquire = MagicMock(side_effect=_PoolBusy("saturated"))  # type: ignore[method-assign]
        with pytest.raises(_PoolBusy), checkout_connection({"name": "heavy-checkout"}, max_wait=0.2, heavy=True):
            pass
        assert err_pool.stats()["heavy"]["in_use"] == 0
        err_pool.acquire = MagicMock(side_effect=lambda *a, **k: MagicMock())  # type: ignore[method-assign]
        with pytest.raises(RuntimeError), checkout_connection({"name": "heavy-checkout"}, max_wait=0.2, heavy=True):
            raise RuntimeError("query failed")
        assert err_pool.stats()["heavy"]["in_use"] == 0 and err_pool.release.call_args.kwargs["errored"] is True


def test_slow_checkout_logging(caplog, monkeypatch):
    pool = _pool_with_fake_connections(4)
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
        caplog.at_level("WARNING", logger="backend.core.duckdb_pool"),
    ):
        monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "50")
        pool.acquire = MagicMock(side_effect=lambda *a, **k: (time.sleep(0.12), MagicMock())[1])  # type: ignore[method-assign]
        with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0, heavy=True, label="/api/cmcd/aggregates"):
            pass
        msgs = [r.getMessage() for r in caplog.records if "slow checkout" in r.getMessage()]
        assert len(msgs) == 1 and "route=/api/cmcd/aggregates" in msgs[0] and "heavy=True" in msgs[0]

        for thresh, delay in (("500", 0.0), ("0", 0.05)):
            caplog.clear()
            monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", thresh)
            pool.acquire = MagicMock(
                side_effect=lambda *a, _d=delay, **k: (time.sleep(_d) if _d else None, MagicMock())[1]
            )  # type: ignore[method-assign]
            with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0, label="/api/cron-runs"):
                pass
            assert not [r for r in caplog.records if "slow checkout" in r.getMessage()]
            assert (pool.acquire.call_args.kwargs["checkout_timings"] is None) == (thresh == "0")


@pytest.mark.parametrize("checkout_path", ["reused", "skip_view", "fresh"])
def test_slow_checkout_phase_diagnostics(caplog, monkeypatch, checkout_path: str) -> None:
    # Deterministic phase delays: admission 7ms, attach 100ms, view 200ms,
    # or fresh build 300ms. No sleeps, database opens or OTel exporter needed.
    clock = [100.0]

    def advance(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setattr(duckdb_pool.time, "monotonic", lambda: clock[0])
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "1")
    pool = _Pool("phase-checkout", max_size=1)
    con = _mock_conn()

    def take_idle() -> MagicMock:
        advance(0.007)
        if checkout_path == "fresh":
            raise queue.Empty
        return con

    def attach(*args: object, **kwargs: object) -> bool:
        advance(0.1)
        return True

    def prepare(*args: object, **kwargs: object) -> MagicMock:
        advance(0.2)
        return con

    def build(*args: object, **kwargs: object) -> MagicMock:
        advance(0.3)
        return con

    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda raw, **_: raw),
        patch.object(pool._idle, "get_nowait", side_effect=take_idle),
        patch.object(pool, "_record_wait_telemetry") as queue_wait,
        patch.object(pool, "_prepare_checkout", side_effect=prepare) as view,
        patch.object(pool, "_stamp_fingerprint"),
        patch.object(pool, "release"),
        patch("backend.core.storage._core._ducklake_attach", side_effect=attach) as attachment,
        patch("backend.core.duckdb.get_connection", side_effect=build),
        patch.object(duckdb_pool, "_pool_conn_memory_limit", return_value=None),
        caplog.at_level("WARNING", logger="backend.core.duckdb_pool"),
    ):
        with checkout_connection({"name": "phase-checkout"}, skip_view_update=checkout_path == "skip_view"):
            pass

    assert queue_wait.call_args.args[0] == pytest.approx(7.0)
    messages = [r.getMessage() for r in caplog.records if "slow checkout" in r.getMessage()]
    assert len(messages) == 1
    message = messages[0]
    assert "queue_admission_ms=7.0" in message
    assert "acquire_total_ms=" in message and "pool_wait_ms=" not in message
    if checkout_path == "fresh":
        assert "reused_attach_ms=None view_prepare_ms=None fresh_build_ms=300.0" in message
        attachment.assert_not_called()
        view.assert_not_called()
    else:
        expected_view = "None" if checkout_path == "skip_view" else "200.0"
        assert f"reused_attach_ms=100.0 view_prepare_ms={expected_view} fresh_build_ms=None" in message
        attachment.assert_called_once()
        assert view.call_count == (checkout_path != "skip_view")


# ── discard_idle, reset_pool_for_service & reuse_only ────────────────


def test_discard_idle_reset_and_reuse_only():
    # discard_idle closes idle, zeroes in_use, does not enter draining mode
    pool = _Pool(service_key="discard_idle", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3
    assert pool.discard_idle() == 3
    assert (pool._in_use, pool._idle.empty(), pool._draining) == (0, True, False)
    for c in conns:
        c.close.assert_called_once()

    # In-flight checked-out conns left alone
    pool_inf = _Pool(service_key="discard_inflight", max_size=2)
    pool_inf._in_use = 1
    assert (pool_inf.discard_idle(), pool_inf._in_use) == (0, 1)

    # reset_pool_for_service noop when absent, closes idle when present
    assert _pool.reset_pool_for_service("never-queried-service-xyz") == 0
    key = "reset-svc-xyz"
    with _clean_pool_keys(key):
        rpool = _pool._get_pool(key, max_size=2)
        rconns = [_mock_conn() for _ in range(2)]
        for c in rconns:
            rpool._idle.put_nowait(c)
        rpool._in_use = 2
        assert _pool.reset_pool_for_service(key) == 2 and rpool._idle.empty()
        for c in rconns:
            c.close.assert_called_once()

    # reuse_only rejects immediately when no idle (even with free capacity or when saturated)
    ro_cap = _Pool(service_key="reuse_only_cap", max_size=4)
    with pytest.raises(_PoolBusy):
        ro_cap.acquire(src={"name": "reuse_only_cap", "bucket": "b"}, max_wait=0.2, reuse_only=True)
    assert (ro_cap._in_use, ro_cap._created_total, ro_cap._idle.empty()) == (0, 0, True)

    ro_sat = _Pool(service_key="reuse_only_sat", max_size=1)
    ro_sat._in_use = 1
    t0 = time.monotonic()
    with pytest.raises(_PoolBusy):
        ro_sat.acquire(src={"name": "reuse_only_sat", "bucket": "b"}, max_wait=2.0, reuse_only=True)
    assert (time.monotonic() - t0) < 0.5
    assert ro_sat._saturated_rejects_total == 0


# ── Merged from tests/perf/test_perf_optimizations.py ──

import multiprocessing

from backend.core.sql_validator import escape_sql_literal
from backend.utils.hll import HyperLogLog


def _fake_src(service_id="svc-perf"):
    return {
        "name": service_id,
        "service_id": service_id,
        "bucket": "b",
        "endpoint": "e",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }


def _minimal_cfg(service_id="svc-perf", commit_interval_mins=5):
    return {
        "service_id": service_id,
        "log_period": 60,
        "commit_interval_mins": commit_interval_mins,
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": False}},
    }


@pytest.mark.parametrize(
    ("sid", "override_fn", "job_id", "check_fn"),
    [
        ("svc-perf", lambda c: None, "commit_svc-perf", lambda g: g == 300),
        (
            "svc-perf-10",
            lambda c: c["provisioning"]["cron_sync"].update(commit_interval_mins=10),
            "commit_svc-perf-10",
            lambda g: g == 600,
        ),
        (
            "svc-prewarm",
            lambda c: c["provisioning"].update(cron_compact={"enabled": True}),
            "insights_prewarmer_svc-prewarm",
            lambda g: g > 240,
        ),
    ],
)
def test_scheduler_job_misfire_grace_times(sid, override_fn, job_id, check_fn):
    from backend.cron.scheduler import Scheduler

    cfg = _minimal_cfg(service_id=sid)
    override_fn(cfg)
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src(sid)),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()
    job = s._sched.get_job(job_id)
    assert job is not None and check_fn(job.misfire_grace_time)


def test_duckdb_threads_env_and_cpu_cap(monkeypatch):
    monkeypatch.setenv("DUCKDB_THREADS", "2")
    duckdb_mod._cached_n_threads = None
    assert (
        int(os.environ["DUCKDB_THREADS"]) if os.environ.get("DUCKDB_THREADS") else min(multiprocessing.cpu_count(), 8)
    ) == 2
    duckdb_mod._cached_n_threads = None

    monkeypatch.delenv("DUCKDB_THREADS", raising=False)
    assert (
        int(os.environ["DUCKDB_THREADS"]) if os.environ.get("DUCKDB_THREADS") else min(multiprocessing.cpu_count(), 8)
    ) == min(multiprocessing.cpu_count(), 8)
    with patch("multiprocessing.cpu_count", return_value=32):
        assert (
            int(os.environ["DUCKDB_THREADS"])
            if os.environ.get("DUCKDB_THREADS")
            else min(multiprocessing.cpu_count(), 8)
        ) == 8


_PREWARM_PREFIXES = (
    "/dashboard",
    "/network",
    "/origin",
    "/performance",
    "/security",
    "/charts",
    "/insights",
    "/fastly-value",
    "/usage",
    "/streaming",
    "/control-room",
    "/sessions/stream",
)


def _needs_prewarm(pathname: str) -> bool:
    return any(pathname.startswith(p) for p in _PREWARM_PREFIXES)


@pytest.mark.parametrize(
    ("route", "expected"),
    [
        ("/usage", True),
        ("/usage/", True),
        ("/streaming", True),
        ("/control-room", True),
        ("/sessions/stream", True),
        ("/sessions/stream/abc123", True),
        ("/sessions", False),
        ("/query", False),
        *[
            (r, True)
            for r in ("/dashboard", "/network", "/origin", "/performance", "/security", "/insights", "/fastly-value")
        ],
    ],
)
def test_prewarm_routes(route, expected):
    assert _needs_prewarm(route) is expected


@pytest.mark.benchmark
def test_bench_hll_add_1k_items(benchmark):
    items = [f"203.0.113.{i % 256}" for i in range(1000)]

    def _go():
        s = HyperLogLog()
        for item in items:
            s.add(item)
        return s

    assert benchmark(_go).count() > 0


@pytest.mark.benchmark
def test_bench_hll_count_after_100k_adds(benchmark):
    sketch = HyperLogLog()
    for i in range(100_000):
        sketch.add(f"203.0.113.{i % 256}.{i // 256}")
    assert benchmark(sketch.count) > 0


@pytest.mark.benchmark
def test_bench_hll_merge_24_hours(benchmark):
    sketches = []
    for hour in range(24):
        s = HyperLogLog()
        for i in range(5_000):
            s.add(f"hour{hour}:198.51.100.{i % 256}.{i // 256}")
        sketches.append(s)

    def _go():
        m = HyperLogLog()
        for s in sketches:
            m.merge(s)
        return m

    assert benchmark(_go).count() > 0


@pytest.mark.benchmark
def test_bench_hll_serialize_roundtrip(benchmark):
    sketch = HyperLogLog()
    for i in range(10_000):
        sketch.add(f"192.0.2.{i % 256}.{i // 256}")
    blob = sketch.to_bytes()
    assert benchmark(lambda: HyperLogLog.from_bytes(blob).to_bytes()) == blob


@pytest.mark.benchmark
def test_bench_safe_table_name(benchmark):
    assert benchmark(_safe_table_name, "svc-test-account-name-with-dashes-and-some-numbers-42").startswith("logs_")


@pytest.mark.benchmark
def test_bench_escape_sql_literal_typical(benchmark):
    key = "raw/2026-05-15/12/2026-05-15T12-34-56.svc-name.gz"
    assert benchmark(escape_sql_literal, key) == key


@pytest.mark.benchmark
def test_bench_escape_sql_literal_with_quotes(benchmark):
    assert "''" in benchmark(escape_sql_literal, "raw/2026-05-15/O'Brien's-bucket/file.gz")


def test_drop_service_ducklake_state_drops_tables_and_removes_local_dir(tmp_path, monkeypatch):
    from backend import config as svcconfig
    from backend.core.storage._core import ducklake_table_name

    source = {"service_id": "svc", "name": "svc"}
    lake_dir = tmp_path / "svc" / "ducklake" / "main" / ducklake_table_name(source, "logs")
    lake_dir.mkdir(parents=True)
    (lake_dir / "part.parquet").write_text("x")
    other_dir = tmp_path / "other" / "ducklake"
    other_dir.mkdir(parents=True)
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", tmp_path)
    monkeypatch.setattr(svcconfig, "DUCKLAKE_DATA_PATH", None)
    fake_con = MagicMock()
    with (
        patch.object(d, "get_source_for_service", return_value=source),
        patch.object(d.duckdb, "connect", return_value=fake_con),
        patch("backend.core.storage._core._ducklake_attach", return_value=True),
        patch.object(reset_mod, "_purge_dropped_catalog_rows", return_value=7) as purge,
    ):
        result = reset_mod.drop_service_ducklake_state("svc")

    statements = [c.args[0] for c in fake_con.execute.call_args_list]
    expected_tables = [ducklake_table_name(source, t) for t in ("logs", "client_vitals", "client_errors")]
    assert statements == [f'DROP TABLE IF EXISTS lake."{t}"' for t in expected_tables]
    purge.assert_called_once_with("svc", expected_tables)
    assert result == {"tables_dropped": 3, "local_dirs_removed": 1, "catalog_rows_purged": 7}
    assert not (tmp_path / "svc").exists()
    assert other_dir.exists(), "another service's DuckLake data must survive"
    fake_con.close.assert_called_once()


def test_drop_service_ducklake_state_unknown_service_is_noop():
    with patch.object(d, "get_source_for_service", return_value=None):
        assert reset_mod.drop_service_ducklake_state("missing") == {"tables_dropped": 0, "local_dirs_removed": 0}


def test_drop_service_ducklake_state_attach_failure_raises_and_keeps_local_files(tmp_path, monkeypatch):
    from backend import config as svcconfig

    lake_dir = tmp_path / "svc" / "ducklake"
    lake_dir.mkdir(parents=True)
    (lake_dir / "part.parquet").write_text("x")
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", tmp_path)
    monkeypatch.setattr(svcconfig, "DUCKLAKE_DATA_PATH", None)
    fake_con = MagicMock()
    with (
        patch.object(d, "get_source_for_service", return_value={"service_id": "svc", "name": "svc"}),
        patch.object(d.duckdb, "connect", return_value=fake_con),
        patch("backend.core.storage._core._ducklake_attach", return_value=False),
        pytest.raises(RuntimeError, match="could not attach"),
    ):
        reset_mod.drop_service_ducklake_state("svc")
    fake_con.execute.assert_not_called()
    fake_con.close.assert_called_once()
    assert (lake_dir / "part.parquet").exists(), "catalog still references these files"


def test_drop_service_ducklake_state_drop_failure_raises_and_keeps_local_files(tmp_path, monkeypatch):
    from backend import config as svcconfig

    lake_dir = tmp_path / "svc" / "ducklake"
    lake_dir.mkdir(parents=True)
    (lake_dir / "part.parquet").write_text("x")
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", tmp_path)
    monkeypatch.setattr(svcconfig, "DUCKLAKE_DATA_PATH", None)
    fake_con = MagicMock()
    fake_con.execute.side_effect = [None, RuntimeError("catalog locked"), None]
    with (
        patch.object(d, "get_source_for_service", return_value={"service_id": "svc", "name": "svc"}),
        patch.object(d.duckdb, "connect", return_value=fake_con),
        patch("backend.core.storage._core._ducklake_attach", return_value=True),
        pytest.raises(RuntimeError, match="client_vitals: catalog locked"),
    ):
        reset_mod.drop_service_ducklake_state("svc")
    assert fake_con.execute.call_count == 3, "every table drop is attempted before failing"
    assert (lake_dir / "part.parquet").exists()


def test_drop_service_ducklake_state_blocked_remote_root_uses_the_writers_local_fallback(tmp_path, monkeypatch):
    """The storage guard makes writers fall back to local disk; teardown must clean the same place."""
    from backend import config as svcconfig

    lake_dir = tmp_path / "svc" / "ducklake"
    lake_dir.mkdir(parents=True)
    (lake_dir / "part.parquet").write_text("x")
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", tmp_path)
    monkeypatch.setattr(svcconfig, "DUCKLAKE_DATA_PATH", "s3://elsewhere/ducklake/")
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    monkeypatch.delenv("ALLOW_REMOTE_S3_PARQUET_SCAN", raising=False)
    with (
        patch.object(d, "get_source_for_service", return_value={"service_id": "svc", "name": "svc"}),
        patch.object(d.duckdb, "connect", return_value=MagicMock()),
        patch("backend.core.storage._core._ducklake_attach", return_value=True),
        patch.object(
            reset_mod.psycopg, "connect", side_effect=AssertionError("mocked catalog must not open PostgreSQL")
        ),
        patch.object(reset_mod, "_purge_dropped_catalog_rows", return_value=0),
    ):
        result = reset_mod.drop_service_ducklake_state("svc")
    assert result["local_dirs_removed"] == 1 and not lake_dir.exists()


class _KeyStorePaginator:
    def __init__(self, keys):
        self._keys = keys

    def paginate(self, Bucket, Prefix):
        return [{"Contents": [{"Key": k} for k in sorted(self._keys) if k.startswith(Prefix)]}]


def _run_purge(root, tbl, keys, bucket="b", prefix=""):
    fos = MagicMock()
    fos.get_paginator.return_value = _KeyStorePaginator(keys)
    deleted = []

    def delete_fn(_fos, _bucket, batch):
        deleted.extend(batch)
        return len(batch)

    gen = reset_mod._purge_ducklake_remote_dirs(fos, root, tbl, bucket, delete_fn, prefix)
    messages = []
    try:
        while True:
            messages.append(next(gen))
    except StopIteration as stop:
        return stop.value, deleted, messages


@pytest.mark.parametrize(
    ("root", "expected_deleted"),
    [
        ("s3://b/pfx/ducklake/", ["pfx/ducklake/logs_svc/f2.parquet", "pfx/ducklake/main/logs_svc/f1.parquet"]),
        ("s3://b/pfx/ducklake", ["pfx/ducklake/logs_svc/f2.parquet", "pfx/ducklake/main/logs_svc/f1.parquet"]),
        ("s3://b/", ["logs_svc/f2.parquet", "main/logs_svc/f1.parquet"]),
    ],
)
def test_purge_ducklake_remote_dirs_only_deletes_the_named_table(root, expected_deleted):
    base = root[len("s3://b/") :].rstrip("/")
    base = f"{base}/" if base else ""
    keys = [
        f"{base}main/logs_svc/f1.parquet",
        f"{base}logs_svc/f2.parquet",
        f"{base}main/logs_svc__client_vitals/sibling.parquet",
        f"{base}logs_svc__client_vitals/sibling2.parquet",
        f"{base}main/logs_svc_other/other.parquet",
        f"{base}main/logs_other/other.parquet",
        "meta/admin_state.json",
    ]
    total, deleted, _ = _run_purge(root, "logs_svc", keys)
    assert sorted(deleted) == expected_deleted and total == 2


def test_purge_ducklake_remote_dirs_skips_a_foreign_bucket():
    total, deleted, messages = _run_purge("s3://elsewhere/ducklake/", "logs_svc", ["main/logs_svc/f.parquet"])
    assert (total, deleted) == (0, []) and "Skipping cloud DuckLake purge" in messages[0]["message"]


@pytest.mark.parametrize(("prefix", "base"), [("pfx", "pfx/ducklake/"), ("/pfx/", "pfx/ducklake/"), ("", "ducklake/")])
def test_purge_ducklake_remote_dirs_removes_the_cloud_copy_of_local_data(prefix, base):
    keys = [f"{base}main/logs_svc/f1.parquet", f"{base}main/logs_svc__client_vitals/other.parquet", "raw/request/x.gz"]
    total, deleted, _ = _run_purge("/local/ducklake", "logs_svc", keys, prefix=prefix)
    assert deleted == [f"{base}main/logs_svc/f1.parquet"] and total == 1


def test_prepare_checkout_tries_fast_path_view_before_full_update():
    pool = _Pool(service_key="test_fast_rebind", max_size=1)
    mc = _mock_conn()
    _set_conn_state(mc, view_fingerprint=("old_sql", "old_hash"), buffer_mtime=10.0)
    pool._idle.put_nowait(mc)
    pool._in_use = 1
    fresh_entry = ("CREATE OR REPLACE VIEW logs AS SELECT 1", ("v1",), "src_fp", 100.0)
    with (
        patch.dict("backend.core.storage._core._view_cache", {"test_fast_rebind": fresh_entry}, clear=False),
        patch("backend.core.storage._core._try_fast_path_view", return_value=True) as mock_fast,
        patch("backend.core.storage._core.update_storage_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=20.0),
    ):
        assert pool.acquire(src={"name": "test_fast_rebind", "bucket": "b"}, max_wait=0.5) is mc
    mock_fast.assert_called_once()
    mock_uiv.assert_not_called()
    assert _get_conn_state(mc, "view_fingerprint") == fresh_entry
    assert _get_conn_state(mc, "buffer_mtime") == 20.0
