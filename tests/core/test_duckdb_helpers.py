"""Tests for ``backend.core.duckdb`` — pure helpers, wrappers, type round-trips, concurrency, and recycle."""

from __future__ import annotations

import gc
import json
import os
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import backend.core.duckdb as d
import backend.core.duckdb as duckdb_mod
import backend.core.duckdb_recycle as rec
from backend.core import iceberg as _ice
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
from backend.core.duckdb_pool import _Pool
from backend.core.iceberg.view import _get_service_lock
from backend.core.log_fields import LOG_FIELD_CATALOG
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
        "access_level": "read_only",
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

    for task in ("commit", "optimize", "expire", "full_sync", "metadata_cleanup", "metadata_sync"):
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
            "backend.core.iceberg.clear_source_caches", side_effect=lambda _k, **_kw: call_order.append("clear")
        ) as mock_clr,
        patch(
            "backend.core.iceberg.update_iceberg_view", side_effect=lambda *_a, **_kw: call_order.append("update")
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
    _ice._view_cache["svc"] = ("loc", set(), tuple(), "SELECT * FROM existing_view", 1.0, False)
    _ice.clear_source_caches("svc", keep_snapshot_cache=True)
    assert "svc" not in _ice._view_cache

    _ice._view_cache["teardown-svc"] = ("loc", set(), tuple(), "SELECT 1", 1.0, False)
    _ice._view_cache["teardown-svc::client_vitals"] = ("loc", set(), tuple(), "SELECT 1", 1.0, False)
    _ice.clear_source_caches("teardown-svc")
    assert "teardown-svc" not in _ice._view_cache and "teardown-svc::client_vitals" not in _ice._view_cache

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
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
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
