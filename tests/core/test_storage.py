"""Tests for DuckLake buffer, view, schema, cloud-write locks, catalog helpers, and local compaction."""

from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import duckdb
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests._helpers.sources import make_source

import backend.core.storage._core as buffer_mod
import backend.core.storage._core as ducklake
from backend import config as svcconfig
from backend.core import duckdb as db
from backend.core import reset as reset_mod
from backend.core import storage
from backend.core import storage as lc
from backend.core.duckdb import get_connection
from backend.core.duckdb_pool import _Pool
from backend.core.ingest import get_catalog_field_ids
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.rollups.recompute import build_time_series_bundles
from backend.core.storage import _compact_single_partition, _core
from backend.core.storage._core import (
    DuckLakeAdmissionTimeout,
    _commit_buffer_impl,
    _ducklake_attach,
    _ducklake_write_connection,
    _get_service_lock,
    _reset_for_tests,
    _source_variant_fp,
    buffer_files,
    ducklake_table_exists,
    ducklake_table_name,
    ducklake_write_admission,
    get_admission_stats,
    get_snapshot_calendar,
    get_table_info,
)

_make_source = partial(make_source, kind="lake", buffer_dir=True)


def _ducklake_source(tmp_path: Path, prefix: str) -> dict:
    name = f"{prefix}{uuid.uuid4().hex[:8]}"
    cache = tmp_path / f"cache-{name}"
    (cache / "buffer").mkdir(parents=True)
    return {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }


def _durable_source(tmp_path: Path) -> dict:
    return {
        "name": "durable-svc",
        "service_id": "durable-svc",
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
        "deployment_mode": "high_scale",
        "bucket": "bucket",
        "prefix": "logs",
    }


def _make_buffer(tmp_path: Path, *names: str) -> tuple[dict, list[str]]:
    src = {"name": "svc-tomb"}
    buf = tmp_path / "buffer"
    buf.mkdir(parents=True, exist_ok=True)
    paths = []
    for n in names:
        p = buf / n
        p.write_bytes(b"")
        paths.append(str(p))
    return src, paths


def _write_buffer_parquet(
    source: dict,
    filename: str,
    rows: int = 1,
    offset: int = 0,
    *,
    ts: datetime | None = None,
    source_file: str | None = None,
    extra: dict | None = None,
) -> Path:
    base = ts or datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
    src_file = source_file or f"s3://b/raw/{filename}"
    cols: dict = {
        "timestamp": pa.array(
            [base + timedelta(seconds=offset + i) for i in range(rows)], type=pa.timestamp("us", tz="UTC")
        ),
        "ip": pa.array([f"10.0.0.{offset + i}" for i in range(rows)]),
        "_source_file": pa.array([src_file] * rows),
    }
    for k, v in (extra or {}).items():
        cols[k] = pa.array([v] * rows)
    path = Path(source["_cache_dir_override"]) / "buffer" / filename
    pq.write_table(pa.table(cols), str(path))
    return path


def _make_committed_source(tmp_path: Path, name: str) -> dict:
    src = _make_source(tmp_path, name)
    _write_buffer_parquet(
        src, "init.parquet", rows=1, ts=datetime(2026, 8, 30, 12, 0, tzinfo=UTC), source_file="s3://b/raw/init.gz"
    )
    _commit_buffer_impl(src)
    return src


def _lake_row_count(source: dict) -> int:
    con = get_connection(source)
    try:
        row = con.execute(f'SELECT count(*) FROM lake."{ducklake_table_name(source)}"').fetchone()
        assert row is not None
        return int(row[0])
    finally:
        con.close()


@contextmanager
def _hold_lock_in_thread(lock, wait_s: float = 5.0):
    held, release = threading.Event(), threading.Event()

    def _hold():
        lock.acquire()
        held.set()
        release.wait(timeout=wait_s)
        lock.release()

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    assert held.wait(timeout=5.0)
    try:
        yield
    finally:
        release.set()
        holder.join(timeout=5.0)


@pytest.fixture
def lock_held_by_other_thread(fos_source):
    with _hold_lock_in_thread(_get_service_lock(fos_source["name"]), wait_s=5.0):
        yield


@pytest.fixture
def reset_admission_state():
    _reset_for_tests()
    yield
    _reset_for_tests()


def _recording_write_connection(monkeypatch, fail_on: str | None = None) -> list[str]:
    executed: list[str] = []

    class _Result:
        def fetchall(self):
            return []

    class _Con:
        def execute(self, sql, *a, **kw):
            executed.append(sql)
            if fail_on and fail_on in sql:
                raise RuntimeError("simulated optimize failure")
            return _Result()

    @contextmanager
    def fake(_source):
        yield _Con()

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", fake)
    monkeypatch.setattr(buffer_mod, "_lake_columns", lambda _con, _tbl: {"timestamp"})
    return executed


class _ExecuteProxy:
    def __init__(self, con, reject_insert):
        self._con = con
        self._reject_insert = reject_insert

    def execute(self, query, *args, **kwargs):
        self._reject_insert(query)
        return self._con.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


def _proxy_commit_connection(monkeypatch, reject_insert):
    original = buffer_mod._ducklake_write_connection

    @contextmanager
    def proxied(source):
        with original(source) as con:
            yield _ExecuteProxy(con, reject_insert)

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", proxied)
    return original


# --- Schema & Field Ordering Tests ---


def test_align_to_schema():
    table = pa.table({"timestamp": [1714819200], "ip": ["1.1.1.1"], "extra_field": ["hidden"]})
    aligned = storage._align_to_schema(table)
    arrow_schema = storage.get_arrow_schema()
    assert len(aligned.schema) == len(arrow_schema)
    assert "timestamp" in aligned.column_names and "ip" in aligned.column_names
    assert "extra_field" not in aligned.column_names
    assert "status" in aligned.column_names and aligned.column("status").null_count == len(aligned)


@pytest.mark.parametrize("cfg", [None, {}, {"custom_fields": []}])
def test_get_arrow_schema_no_custom_fields(cfg):
    base_schema = storage.get_arrow_schema()
    schema = storage.get_arrow_schema(cfg)
    assert isinstance(schema, pa.Schema) and schema == base_schema
    field_names = {f.name for f in schema}
    assert "timestamp" in field_names and "status" in field_names


@pytest.mark.parametrize(
    ("field_name", "duckdb_type", "expected_pa_type"),
    [
        ("my_field", "VARCHAR", pa.string()),
        ("cf_int", "INTEGER", pa.int32()),
        ("cf_bigint", "BIGINT", pa.int64()),
        ("cf_double", "DOUBLE", pa.float64()),
    ],
)
def test_get_arrow_schema_custom_field_types(field_name, duckdb_type, expected_pa_type):
    config = {"custom_fields": [{"name": field_name, "duckdb_type": duckdb_type, "enabled": True}]}
    schema = storage.get_arrow_schema(config)
    field_names = [f.name for f in schema]
    assert field_name in field_names
    assert field_names.index(field_name) > field_names.index("timestamp")
    assert schema.field(field_name).type == expected_pa_type


def test_get_arrow_schema_enabled_vs_disabled_and_sort_order():
    config = {
        "groups": ["A"],
        "field_overrides": {},
        "custom_fields": [
            {
                "name": "zzz_last",
                "duckdb_type": "VARCHAR",
                "storage_type": "string",
                "vcl_log_expression": "X",
                "enabled": True,
            },
            {
                "name": "aaa_first",
                "duckdb_type": "VARCHAR",
                "storage_type": "string",
                "vcl_log_expression": "X",
                "enabled": True,
            },
            {
                "name": "inactive_field",
                "duckdb_type": "VARCHAR",
                "storage_type": "string",
                "vcl_log_expression": "X",
                "enabled": False,
            },
        ],
    }
    schema = storage.get_arrow_schema(config)
    field_names = [f.name for f in schema]
    assert "aaa_first" in field_names and "zzz_last" in field_names
    assert "inactive_field" not in field_names
    assert field_names.index("aaa_first") < field_names.index("zzz_last")


def test_field_order_covers_ingest_storage_fields():
    storage_ids = set(get_catalog_field_ids())
    order_names = set(_core._FIELD_ORDER)
    assert not (storage_ids - order_names)
    internal_ids = {f["id"] for f in LOG_FIELD_CATALOG if f.get("group") == "INTERNAL"}
    assert not (order_names - storage_ids - (_core._RESERVED_FIELD_SLOTS | internal_ids))
    schema_names = {f.name for f in storage.get_arrow_schema()}
    assert not (storage_ids - schema_names)


# --- Catalog Initialization & View Contention Tests ---


def test_init_storage_table_attaches_catalog_without_creating_table(tmp_path):
    source = _ducklake_source(tmp_path, "init")
    assert storage.init_storage_table(source) is True
    assert storage.init_storage_table(source, create=True) is True
    assert ducklake_table_exists(source) is False


def test_init_storage_table_returns_none_when_catalog_unattachable(tmp_path):
    source = _ducklake_source(tmp_path, "initfail")
    fake_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.storage._core._ducklake_attach", return_value=False),
    ):
        assert storage.init_storage_table(source) is None
    fake_con.close.assert_called_once()


def test_duckdb_databases_readonly_detection_against_real_connection(tmp_path):
    db_path = str(tmp_path / "ro_detect.duckdb")
    w = duckdb.connect(db_path)
    w.execute("CREATE TABLE t(x INT)")
    w.close()

    rw = duckdb.connect(db_path)
    rw_row = rw.execute(
        "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp') LIMIT 1"
    ).fetchone()
    rw.close()
    assert rw_row is not None and rw_row[0] is False

    ro = duckdb.connect(db_path, read_only=True)
    ro_row = ro.execute(
        "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp') LIMIT 1"
    ).fetchone()
    assert ro_row is not None and ro_row[0] is True
    ro.execute("CREATE OR REPLACE TEMP VIEW vtmp AS SELECT * FROM t")
    assert ro.execute("SELECT COUNT(*) FROM vtmp").fetchone()[0] == 0
    ro.close()


@pytest.mark.parametrize(
    ("source_key", "persisted_row", "hold_s", "lock_timeout", "expected_calls"),
    [
        ("lock-contention-fresh-svc", None, 0.1, 0.02, 1),
        ("lock-contention-persistent-view-svc", (1,), 5.0, 0.2, 0),
    ],
)
def test_update_storage_view_lock_contention(
    monkeypatch, source_key, persisted_row, hold_s, lock_timeout, expected_calls
):
    storage._view_cache.pop(source_key, None)
    storage._service_locks.pop(source_key, None)
    source = {"name": source_key}
    locked_calls: list = []
    monkeypatch.setattr(storage, "_update_storage_view_locked", lambda con, src: locked_calls.append((con, src)))

    lock = storage._get_service_lock(source_key)
    fake_con = MagicMock()
    fake_con.execute.return_value.fetchone.return_value = persisted_row
    try:
        with _hold_lock_in_thread(lock, wait_s=hold_s):
            storage.update_storage_view(fake_con, source, lock_timeout=lock_timeout)
        assert len(locked_calls) == expected_calls
    finally:
        storage._view_cache.pop(source_key, None)
        storage._service_locks.pop(source_key, None)


def test_source_variant_fp_changes_when_time_range_or_cron_changes():
    base = {"name": "svc-002"}
    assert _source_variant_fp(base) == ((), True)

    bounded = {
        "name": "svc-002",
        "time_range": {"start": "2026-06-01T00:00:00Z", "end": "2026-06-02T00:00:00Z"},
    }
    fp_bounded = _source_variant_fp(bounded)
    assert fp_bounded != _source_variant_fp(base)
    assert fp_bounded[0] == ("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z")

    cron_disabled = {"name": "svc-002", "provisioning": {"cron_sync": {"enabled": False}}}
    assert _source_variant_fp(cron_disabled) != _source_variant_fp(base)


# --- Compaction & Cloud Write Lock Serialization Tests ---


def test_optimize_table_hostile_service_id_cannot_break_out_of_call_literal(monkeypatch):
    executed = _recording_write_connection(monkeypatch)
    source = {"name": "svc", "service_id": "O'Brien'); DROP TABLE lake.logs;--"}
    storage.optimize_table(source, table_name="logs")

    calls = [q for q in executed if q.startswith("CALL ducklake_merge_adjacent_files")]
    assert calls
    for q in executed:
        if q.startswith(("CALL ducklake_merge_adjacent_files", "CALL ducklake_rewrite_data_files")):
            assert re.fullmatch(r"CALL ducklake_\w+\('lake', '[a-z0-9_]+'\)", q)
    assert not any("DROP" in q for q in executed)


def test_optimize_table_compacts_schema_evolved_files(tmp_path):
    source = _ducklake_source(tmp_path, "evolve")
    _write_buffer_parquet(source, "batch_old_schema.parquet", rows=2, offset=0)
    assert _commit_buffer_impl(source)["rows_committed"] == 2
    _write_buffer_parquet(source, "batch_new_schema.parquet", rows=2, offset=10, extra={"edge_sid": "evolved"})
    assert _commit_buffer_impl(source)["rows_committed"] == 2

    result = storage.optimize_table(source)
    assert not result.get("error") and result["partition_errors"] == []

    con = get_connection(source)
    try:
        rows = con.execute(f'SELECT ip, edge_sid FROM lake."{ducklake_table_name(source)}" ORDER BY ip').fetchall()
    finally:
        con.close()
    assert rows == [("10.0.0.0", None), ("10.0.0.1", None), ("10.0.0.10", "evolved"), ("10.0.0.11", "evolved")]


def test_optimize_table_merge_failure_is_recorded_not_retried_and_rewrite_still_runs(monkeypatch):
    executed = _recording_write_connection(monkeypatch, fail_on="ducklake_merge_adjacent_files")
    result = storage.optimize_table({"name": "svc", "service_id": "svc-opt"}, table_name="logs")

    merges = [q for q in executed if "ducklake_merge_adjacent_files" in q and q.endswith("'logs_svc_opt')")]
    rewrites = [q for q in executed if "ducklake_rewrite_data_files" in q and q.endswith("'logs_svc_opt')")]
    assert len(merges) == 1 and len(rewrites) == 1
    assert any("merge" in e for e in result["partition_errors"])


@pytest.mark.parametrize(
    ("fn_name", "impl_name", "expected_check"),
    [
        (
            "commit_buffer",
            "_commit_buffer_impl",
            lambda r: r == {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0},
        ),
        ("optimize_table", "_optimize_table_impl", lambda r: r["files_rewritten"] == 0 and "busy" in r["error"]),
        ("run_cloud_maintenance", "_run_cloud_maintenance_impl", lambda r: "busy" in r["error"]),
    ],
)
def test_cloud_writer_skips_cleanly_when_lock_held(
    monkeypatch, fos_source, lock_held_by_other_thread, fn_name, impl_name, expected_check
):
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.1)
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, impl_name, impl)
    result = getattr(buffer_mod, fn_name)(fos_source)
    impl.assert_not_called()
    assert expected_check(result)


def test_commit_buffer_calls_impl_when_lock_free(monkeypatch, fos_source):
    ret = {"files_committed": 3, "rows_committed": 30, "snapshot_id": 1, "quarantined_files": 0}
    impl = MagicMock(return_value=ret)
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)
    result = buffer_mod.commit_buffer(fos_source)
    impl.assert_called_once_with(fos_source, None, table_name="logs")
    assert result["files_committed"] == 3
    lock = _get_service_lock(fos_source["name"])
    assert lock.acquire(timeout=0.1)
    lock.release()


def test_reset_excludes_a_concurrent_commit_attempt(monkeypatch, s3_mock, fos_source):
    monkeypatch.setattr("backend.core.storage._core.init_storage_table", MagicMock())
    monkeypatch.setattr("backend.core.storage._core.update_storage_view", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.get_connection", MagicMock())
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.2)

    svcconfig.save_config(
        fos_source["service_id"],
        {
            "service_id": fos_source["service_id"],
            "name": fos_source["name"],
            "fos_bucket": fos_source["bucket"],
            "fos_prefix": "",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test-key",
            "fos_secret_access_key": "test-secret",
            "provisioning": {"cron_sync": {"enabled": True}},
        },
    )
    reached_middle, release_reset = threading.Event(), threading.Event()

    def _slow_delete(*args, **kwargs):
        reached_middle.set()
        release_reset.wait(timeout=5)
        return 0

    monkeypatch.setattr("backend.core.ingest._delete_objects_robust", _slow_delete)
    s3_mock.put_object(Bucket=fos_source["bucket"], Key="errors/foo.bad.jsonl", Body=b"x")
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)

    reset_thread = threading.Thread(
        target=lambda: list(reset_mod.reset_service_logs(fos_source["service_id"], reload_scheduler=MagicMock())),
        daemon=True,
    )
    reset_thread.start()
    try:
        assert reached_middle.wait(timeout=30)
        real_source = db.get_source_for_service(fos_source["service_id"])
        result = buffer_mod.commit_buffer(real_source)
        impl.assert_not_called()
        assert result["files_committed"] == 0
    finally:
        release_reset.set()
        reset_thread.join(timeout=30)


# --- Buffer, Tombstone, Sweep & Quarantine Tests ---


def test_buffer_dir(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value="cache/test-svc"):
        assert storage._buffer_dir({"name": "test-svc"}) == "cache/test-svc/buffer"
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        assert storage._buffer_dir({"name": "svc"}) == str(tmp_path / "buffer")


def test_tombstone_buffer_files_writes_sidecar_marker_and_leaves_parquet(tmp_path):
    src, paths = _make_buffer(tmp_path, "batch_A.parquet", "batch_B.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        tombstoned = storage.tombstone_buffer_files(src, paths, ts=1717_000_000)
    assert tombstoned == paths
    for p in paths:
        assert os.path.exists(p) and os.path.exists(p + ".consumed-1717000000")


@pytest.mark.parametrize(
    ("batch_name", "ts", "exc"),
    [
        ("batch_fail.parquet", 3_000_000_000, PermissionError("simulated tombstone creation failure")),
        ("batch_failwrite.parquet", 1_717_000_000, OSError("simulated EROFS")),
    ],
)
def test_tombstone_buffer_files_logs_and_skips_without_unlinking_on_failure(tmp_path, batch_name, ts, exc):
    src, paths = _make_buffer(tmp_path, batch_name)
    target_marker = f"{paths[0]}.consumed-{ts}"
    real_open = open

    def _selective_boom(file, mode="r", *args, **kwargs):
        if file == target_marker and mode == "x":
            raise exc
        return real_open(file, mode, *args, **kwargs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("builtins.open", side_effect=_selective_boom),
    ):
        tombstoned = storage.tombstone_buffer_files(src, paths, ts=ts)
    assert os.path.exists(paths[0]) and paths[0] not in tombstoned


def test_buffer_files_excludes_tombstoned_parquets_and_markers(tmp_path):
    src, paths = _make_buffer(tmp_path, "batch_keep.parquet", "batch_consumed.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        storage.tombstone_buffer_files(src, [paths[1]], ts=1717_000_000)
        assert os.path.exists(paths[1] + ".consumed-1717000000")
        listing = storage.buffer_files(src)
    assert listing == [paths[0]] and all(not entry.endswith("1717000000") for entry in listing)


@pytest.mark.parametrize(
    ("batch_name", "now", "expected_swept", "should_exist"),
    [("batch_young.parquet", 2_000_000_010, 0, True), ("batch_old.parquet", 2_000_000_090, 1, False)],
)
def test_sweep_tombstoned_buffer_files_grace_window(tmp_path, batch_name, now, expected_swept, should_exist):
    src, paths = _make_buffer(tmp_path, batch_name)
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        storage.tombstone_buffer_files(src, paths, ts=2_000_000_000)
        swept = storage.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=now)
    assert swept == expected_swept
    assert os.path.exists(paths[0]) is should_exist
    assert os.path.exists(paths[0] + ".consumed-2000000000") is should_exist


def test_sweep_tolerates_malformed_marker_filenames(tmp_path):
    src, _ = _make_buffer(tmp_path, "batch_X.parquet")
    buf = tmp_path / "buffer"
    (buf / "batch_X.parquet.consumed-notanumber").write_bytes(b"")
    (buf / "batch_X.parquet.consumed-").write_bytes(b"")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        assert storage.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_000) == 0


def test_tombstone_then_query_race_keeps_parquet_readable_during_grace(tmp_path):
    src = {"name": "svc-race"}
    buf = tmp_path / "buffer"
    buf.mkdir(parents=True, exist_ok=True)
    parquet_path = str(buf / "batch_race.parquet")
    pq.write_table(pa.table({"v": [1, 2, 3]}), parquet_path)

    con = duckdb.connect(":memory:")
    try:
        with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
            con.execute(f"CREATE OR REPLACE VIEW race_view AS SELECT * FROM read_parquet('{parquet_path}')")
            storage.tombstone_buffer_files(src, [parquet_path], ts=2_000_000_000)
            assert con.execute("SELECT count(*) FROM race_view").fetchone() == (3,)
            assert storage.buffer_files(src) == []
            swept = storage.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_090)
            assert swept == 1 and not os.path.exists(parquet_path)
    finally:
        con.close()


# --- View Caches, Locks & Debug Overlay Helpers ---


def test_clear_and_invalidate_service_caches():
    storage._view_cache["test-svc"] = ("loc", set(), (), "sql", 1.0, True)
    storage.clear_source_caches("test-svc")
    assert "test-svc" not in storage._view_cache
    storage.clear_source_caches("never-seen-svc")

    src = {"name": "inv-test-svc", "bucket": "inv-test-bucket", "prefix": ""}
    _core._view_cache[src["name"]] = ("loc", set(), (), "sql", 1.0, True)
    storage.invalidate_service_caches(src)
    assert src["name"] not in _core._view_cache
    storage.invalidate_service_caches({"name": "never-seen", "bucket": "nb", "prefix": ""})


def test_get_service_lock_identity_and_isolation():
    assert _get_service_lock("svc-X") is _get_service_lock("svc-X")
    assert _get_service_lock("svc-A") is not _get_service_lock("svc-B")


def test_get_last_view_stats():
    storage._view_cache.pop("never-seen", None)
    assert storage.get_last_view_stats({"name": "never-seen"}) == {}
    storage._view_cache["test-svc-stats"] = (
        "metadata_loc",
        frozenset(),
        ("col1", "col2"),
        "CREATE VIEW x AS ...",
        12.3,
        True,
    )
    assert storage.get_last_view_stats({"name": "test-svc-stats"}) == {
        "sql": "CREATE VIEW x AS ...",
        "time_ms": 12.3,
        "was_fast_path": True,
    }


@pytest.mark.parametrize(
    ("svc_name", "was_fast_path", "time_ms", "expected_label"),
    [("test-svc-inject", True, 5.0, "FAST PATH"), ("test-svc-slow", False, 50.0, "SLOW PATH")],
)
def test_inject_view_debug_prepends_formatted_row(svc_name, was_fast_path, time_ms, expected_label):
    storage._view_cache[svc_name] = ("loc", frozenset(), (), "SELECT 1", time_ms, was_fast_path)
    debug_list = [{"sql": "user query", "time_ms": 100}]
    storage.inject_view_debug(debug_list, {"name": svc_name})
    assert "View Resolution" in debug_list[0]["sql"] and expected_label in debug_list[0]["sql"]
    assert debug_list[1]["sql"] == "user query"


def test_inject_view_debug_is_noop_when_no_cached_stats():
    storage._view_cache.pop("never-seen-inject", None)
    debug_list = [{"sql": "user query", "time_ms": 100}]
    storage.inject_view_debug(debug_list, {"name": "never-seen-inject"})
    assert debug_list == [{"sql": "user query", "time_ms": 100}]


# --- Buffer File Listing, Writing & Commit Mechanics ---


def test_buffer_files_missing_sorted_and_recursive(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        assert buffer_files({"name": "svc"}) == []

    buf = tmp_path / "buffer"
    buf.mkdir()
    for name in ("c.parquet", "a.parquet", "b.parquet", "ignore.txt"):
        (buf / name).write_bytes(b"")

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        files = buffer_files({"name": "svc"})
    assert [os.path.basename(f) for f in files] == ["a.parquet", "b.parquet", "c.parquet"]

    nested = tmp_path / "buffer" / "rum_perf" / "2026" / "05" / "18"
    nested.mkdir(parents=True)
    (nested / "deep.parquet").write_bytes(b"")
    (tmp_path / "buffer" / "rum_perf" / "shallow.parquet").write_bytes(b"")

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        rum_files = buffer_files({"name": "svc"}, table_name="rum_perf")
    rum_names = [os.path.basename(f) for f in rum_files]
    assert "deep.parquet" in rum_names and "shallow.parquet" in rum_names


def test_buffer_files_and_sweep_respect_table_name_for_recursive_glob(tmp_path, monkeypatch):
    buf = tmp_path / "buffer" / "rum_events" / "sub"
    buf.mkdir(parents=True)
    target = buf / "ev1.parquet"
    target.write_bytes(b"x")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(tmp_path))
    assert str(target) in buffer_mod.buffer_files({"name": "svc"}, table_name="rum_events")

    marker = buf / "ev1.parquet.consumed-100"
    marker.write_text("")
    swept = buffer_mod.sweep_tombstoned_buffer_files(
        {"name": "svc"}, grace_seconds=10, now=500, table_name="rum_events"
    )
    assert swept == 1 and not target.exists() and not marker.exists()


def test_write_to_buffer_creates_dir_and_uses_zstd_level_1(tmp_path):
    target = tmp_path / "fresh-cache"
    assert not target.exists()
    fake_table = pa.table({"timestamp": pa.array([], type=pa.timestamp("us", tz="UTC"))})
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(target)),
        patch("backend.core.storage._align_to_schema", return_value=fake_table),
        patch("backend.core.storage._core.pq.write_table") as mock_write,
        patch("os.rename"),
    ):
        out = storage.write_to_buffer({"name": "svc"}, fake_table, "x.parquet")
    assert (target / "buffer").exists() and out == str(target / "buffer" / "x.parquet")
    mock_write.assert_called_once()
    _, kwargs = mock_write.call_args
    assert kwargs.get("compression") == "zstd" and kwargs.get("compression_level") == 1


def test_commit_buffer_returns_zero_summary_when_no_buffer_files(tmp_path):
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.storage._init_storage_table_locked") as mock_init,
    ):
        out = storage.commit_buffer({"name": "svc"})
    assert out == {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}
    mock_init.assert_not_called()


def test_quarantine_buffer_file_moves_with_sidecar_and_handles_collision(tmp_path):
    cache_root = tmp_path / "cache-root"
    buffer_dir = cache_root / "bkt" / "buffer"
    buffer_dir.mkdir(parents=True)
    bad = buffer_dir / "batch_deadbeef.parquet"
    bad.write_bytes(b"not a parquet file")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        new_path = storage._quarantine_buffer_file({"name": "svc"}, str(bad), RuntimeError("parquet footer is invalid"))

    assert new_path is not None and ".quarantine" in new_path
    assert not bad.exists() and os.path.exists(new_path)
    sidecar = new_path + ".json"
    assert os.path.exists(sidecar)
    sidecar_data = json.loads(Path(sidecar).read_text())
    assert sidecar_data["error_type"] == "RuntimeError" and "footer is invalid" in sidecar_data["error_message"]
    assert sidecar_data["original_path"] == str(bad)

    bad1 = buffer_dir / "batch_x.parquet"
    bad1.write_bytes(b"first bad")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        p1 = storage._quarantine_buffer_file({"name": "svc"}, str(bad1), ValueError("first"))
    bad2 = buffer_dir / "batch_x.parquet"
    bad2.write_bytes(b"second bad")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        p2 = storage._quarantine_buffer_file({"name": "svc"}, str(bad2), ValueError("second"))
    assert p1 != p2 and os.path.exists(p1) and os.path.exists(p2)


def test_commit_buffer_quarantines_unreadable_files_and_ignores_on_next_run(tmp_path):
    source = _ducklake_source(tmp_path, "qh")
    good = _write_buffer_parquet(source, "batch_good.parquet", rows=3)
    buffer_dir = good.parent
    bad = buffer_dir / "batch_bad.parquet"
    bad.write_bytes(b"definitely not parquet")

    result = storage.commit_buffer(source)
    assert result["files_committed"] == 1 and result["quarantined_files"] == 1 and result["rows_committed"] == 3
    assert _lake_row_count(source) == 3
    assert good.exists() and len(list(good.parent.glob(good.name + ".consumed-*"))) == 1
    assert not bad.exists()
    quarantine_dir = buffer_dir / ".quarantine"
    assert len(list(quarantine_dir.glob("*batch_bad.parquet"))) == 1
    assert len(list(quarantine_dir.glob("*.parquet.json"))) == 1

    _write_buffer_parquet(source, "batch_good_2.parquet", rows=1, offset=10)
    second = storage.commit_buffer(source)
    assert second["quarantined_files"] == 0 and second["files_committed"] == 1 and _lake_row_count(source) == 4


def test_commit_buffer_chunks_commits_when_files_exceed_chunk_size(tmp_path, monkeypatch):
    monkeypatch.setattr(buffer_mod, "_BUFFER_COMMIT_CHUNK_SIZE", 3)
    attaches = {"n": 0}
    orig = buffer_mod._ducklake_write_connection

    def counting(source):
        attaches["n"] += 1
        return orig(source)

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", counting)
    source = _ducklake_source(tmp_path, "ck")
    paths = [_write_buffer_parquet(source, f"batch_{i:02d}.parquet", rows=1, offset=i) for i in range(7)]

    result = storage.commit_buffer(source)
    assert attaches["n"] == 3 and result["files_committed"] == 7 and result["rows_committed"] == 7
    assert _lake_row_count(source) == 7
    for p in paths:
        assert list(p.parent.glob(p.name + ".consumed-*"))
    assert storage.buffer_files(source) == []


def test_buffer_backlog_stats_empty_and_populated(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "empty")):
        assert storage.buffer_backlog_stats({"name": "svc"}) == {
            "file_count": 0,
            "total_bytes": 0,
            "oldest_age_seconds": 0,
            "oldest_path": None,
        }

    cache_root = tmp_path / "svc"
    buf = cache_root / "buffer"
    buf.mkdir(parents=True)
    f1, f2 = buf / "batch_a.parquet", buf / "batch_b.parquet"
    f1.write_bytes(b"a" * 100)
    f2.write_bytes(b"b" * 200)
    past = time.time() - 600
    os.utime(str(f1), (past, past))

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        stats = storage.buffer_backlog_stats({"name": "svc"})
    assert stats["file_count"] == 2 and stats["total_bytes"] == 300
    assert stats["oldest_age_seconds"] >= 590 and stats["oldest_path"] == str(f1)


# --- DuckLake Admission, Attach, Pool & End-to-End Commit Tests ---


@pytest.mark.usefixtures("reset_admission_state")
def test_local_admission_serializes_same_service_and_isolates_different_services():
    entered, release = threading.Event(), threading.Event()
    order: list[str] = []

    def first() -> None:
        with ducklake_write_admission("admission-local"):
            order.append("first")
            entered.set()
            release.wait(timeout=2)

    worker = threading.Thread(target=first)
    worker.start()
    assert entered.wait(timeout=2)

    with pytest.raises(DuckLakeAdmissionTimeout), ducklake_write_admission("admission-local", timeout_s=0.02):
        pass

    started = time.monotonic()
    with ducklake_write_admission("admission-other-service", timeout_s=0.2):
        assert (time.monotonic() - started) < 0.15

    release.set()
    worker.join(timeout=2)
    stats = get_admission_stats()["admission-local"]
    assert order == ["first"] and stats["acquisitions"] == 1 and stats["timeouts"] == 1


@pytest.mark.usefixtures("reset_admission_state")
def test_postgres_admission_fair_lock_and_timeout():
    calls: list[tuple[str, object]] = []

    class Cursor:
        def __init__(self, fail_lock=False):
            self.fail_lock = fail_lock

        def execute(self, sql, params=None):
            calls.append((sql, params))
            if self.fail_lock and "pg_advisory_lock(" in sql:
                raise psycopg.errors.LockNotAvailable("timeout")
            return self

        def fetchone(self):
            return (True,)

    with patch("backend.core.metadata.base.get_pg_thread_connection", return_value=Cursor()):
        with ducklake_write_admission("admission-postgres", timeout_s=0.2):
            pass

    sqls = [c[0] for c in calls]
    assert not any("pg_try_advisory_lock" in s for s in sqls)
    assert any(s.startswith("SET lock_timeout") for s in sqls)
    acq_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_lock(" in s)
    rel_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_unlock(" in s)
    assert acq_idx < rel_idx and calls[acq_idx][1][0] == calls[rel_idx][1][0]

    calls.clear()
    with patch("backend.core.metadata.base.get_pg_thread_connection", return_value=Cursor(fail_lock=True)):
        with pytest.raises(DuckLakeAdmissionTimeout), ducklake_write_admission("admission-pg-timeout", timeout_s=0.1):
            pass
    assert any(s == "SET lock_timeout = 0" for s in [c[0] for c in calls])
    assert get_admission_stats()["admission-pg-timeout"]["timeouts"] == 1


@pytest.mark.usefixtures("reset_admission_state")
def test_duckdb_connect_extension_options(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/duck'lake/ducklake.duckdb_extension")
    connect = Mock(return_value=object())
    monkeypatch.setattr(db.duckdb, "connect", connect)
    db._duckdb_connect("service.duckdb", read_only=False, config={"threads": "2"})
    connect.assert_called_once_with(
        "service.duckdb", read_only=False, config={"threads": "2", "allow_unsigned_extensions": "true"}
    )

    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    connect.reset_mock()
    db._duckdb_connect(":memory:")
    connect.assert_called_once_with(":memory:")


@pytest.mark.usefixtures("reset_admission_state")
def test_ducklake_attach_extension_options(monkeypatch, tmp_path):
    statements: list[str] = []

    class FakeConn:
        def execute(self, stmt):
            statements.append(stmt)
            if stmt.startswith("LOAD"):
                raise RuntimeError("load failed")
            return Mock(fetchone=Mock(return_value=None))

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", "postgres:dbname=test")
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/duck'lake/ducklake.duckdb_extension")
    monkeypatch.delenv("DUCKDB_EXTENSION_DIRECTORY", raising=False)
    assert ducklake._ducklake_attach(FakeConn(), {"service_id": "svc"}) is False
    assert statements == ["LOAD '/opt/duck''lake/ducklake.duckdb_extension';"]

    catalog_path = tmp_path / "lake.ducklake"
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", f"ducklake:{catalog_path}")
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(tmp_path / "data"))
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    con = duckdb.connect()
    try:
        assert ducklake._ducklake_attach(con, {"service_id": "test_direct", "name": "test_direct"}, read_only=False)
        con.execute("CREATE TABLE IF NOT EXISTS lake.t_ext_opts (id INT)")
        con.execute("INSERT INTO lake.t_ext_opts VALUES (1), (2)")
        assert (
            con.execute(
                "SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = 't_ext_opts'"
            ).fetchone()[0]
            > 0
        )
    finally:
        con.close()


def test_drop_service_ducklake_state_drops_real_tables_and_files(monkeypatch, tmp_path):
    import psycopg

    import backend.core.duckdb as core_duckdb
    from backend.core.storage._core import _resolve_catalog_dsn, ducklake_table_name

    data_dir = tmp_path / "data"
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(data_dir))
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    mine, other = {"service_id": "svc_drop", "name": "svc_drop"}, {"service_id": "svc_keep", "name": "svc_keep"}
    names = {ducklake_table_name(mine, t) for t in ("logs", "client_vitals", "client_errors")}
    keep = ducklake_table_name(other, "logs")
    con = duckdb.connect()
    try:
        assert ducklake._ducklake_attach(con, mine, read_only=False)
        for name in (*names, keep):
            con.execute(f'DROP TABLE IF EXISTS lake."{name}"')
            con.execute(f'CREATE TABLE lake."{name}" AS SELECT i AS a FROM range(100) t(i)')
        assert list(data_dir.rglob("*.parquet"))
    finally:
        con.close()

    pg_dsn = _resolve_catalog_dsn("svc_drop").removeprefix("postgres:")

    def catalog_rows(table_names):
        """Rows still keyed to these tables across every catalog child table."""
        with psycopg.connect(pg_dsn) as pg:
            ids = [
                r[0]
                for r in pg.execute(
                    "SELECT table_id FROM ducklake_table WHERE table_name = ANY(%s)", (list(table_names),)
                )
            ]
            children = [
                r[0]
                for r in pg.execute(
                    "SELECT table_name FROM information_schema.columns WHERE table_schema = 'public' "
                    "AND table_name LIKE 'ducklake\\_%' AND column_name = 'table_id'"
                )
            ]
            return sum(
                pg.execute(f'SELECT count(*) FROM "{c}" WHERE table_id = ANY(%s)', (ids,)).fetchone()[0]
                for c in children
            )

    assert catalog_rows(names) and catalog_rows([keep])
    keep_rows_before = catalog_rows([keep])

    with patch.object(core_duckdb, "get_source_for_service", return_value=mine):
        result = core_duckdb.drop_service_ducklake_state("svc_drop")
    assert result["tables_dropped"] == 3
    assert result["catalog_rows_purged"] > 0

    con = duckdb.connect()
    try:
        assert ducklake._ducklake_attach(con, mine, read_only=False)
        remaining = {
            r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'").fetchall()
        }
        assert con.execute(f'SELECT count(*) FROM lake."{keep}"').fetchone()[0] == 100
        con.execute(f'DROP TABLE IF EXISTS lake."{keep}"')
        # A catalog-wide expiry still works, i.e. the purge left no orphaned rows behind.
        con.execute("CALL ducklake_expire_snapshots('lake', older_than => now() + INTERVAL 1 DAY)")
    finally:
        con.close()
    assert not names & remaining, "the torn-down service's tables are dropped"
    assert keep in remaining, "other services' tables survive"
    assert catalog_rows(names) == 0, "no catalog rows of the torn-down service are left"
    assert keep_rows_before > 0
    assert not any(p.parent.name in names for p in data_dir.rglob("*.parquet"))
    assert any(p.parent.name == keep for p in data_dir.rglob("*.parquet")), "other services' data files survive"


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_serving_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")
    temp_dir = tmp_path / "duckdb-temp"
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(temp_dir))
    source = _durable_source(tmp_path)

    with (
        patch("backend.core.duckdb._configure_fos"),
        patch("backend.core.storage.configure_duckdb_s3"),
        patch("backend.core.storage._core._ducklake_attach", return_value=True),
        patch("backend.core.storage.update_storage_view"),
    ):
        con = db.get_connection(source=source, read_only=True, skip_view_update=False)
    try:
        assert db.db_path_for_source(source) == ":memory:durable-svc"
        assert not (tmp_path / "must-not-open.duckdb").exists()
        assert temp_dir.is_dir() and con.execute("SELECT 42").fetchone() == (42,)
    finally:
        con.close()

    with pytest.raises(RuntimeError, match="only permits read-only"):
        db.get_connection(source=source, read_only=False)

    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    source["deployment_mode"] = "standard"
    assert db.db_path_for_source(source) == str(tmp_path / "must-not-open.duckdb")


def test_reader_reuses_live_read_write_attach_and_pool_keeps_sibling_attached(tmp_path):
    name = f"rwreuse{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    with _ducklake_write_connection(src) as writer:
        assert writer.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone() == (
            False,
        )
        reader = get_connection(source=src, read_only=True)
        try:
            assert writer.execute(
                "SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'"
            ).fetchone() == (False,)
            assert _ducklake_attach(reader, src, read_only=True) is True
        finally:
            reader.close()

    pool = _Pool(name, max_size=2)
    first = pool.acquire(src, max_wait=10, skip_view_update=True)
    second = pool.acquire(src, max_wait=10, skip_view_update=True)
    try:
        pool.release(first)
        assert second.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()[0] >= 1
    finally:
        pool.release(second)
        for con in list(pool._idle.queue):
            con.close()

    with closing(get_connection(source=src, read_only=True)) as c2:
        assert c2.execute("SELECT 1").fetchone()[0] == 1


def test_ducklake_detach_lock_and_retry_backoff(monkeypatch):
    monkeypatch.setattr(ducklake, "_ATTACH_CONFLICT_RETRY_SLEEP_S", 0.05)
    monkeypatch.setattr(ducklake, "_ATTACH_CONFLICT_RETRY_ATTEMPTS", 5)

    class FakeConn:
        def __init__(self):
            self.commands: list[str] = []
            self.attempts = 0

        def execute(self, sql: str):
            self.commands.append(sql)
            if sql.startswith("ATTACH 'ducklake:") and " AS lake " in sql:
                self.attempts += 1
                if self.attempts <= 2:
                    raise RuntimeError("Unique file handle conflict: already attached")
            return self

        def fetchone(self):
            return None

    fc = FakeConn()
    assert ducklake._ducklake_attach(fc, {"service_id": "test"}, read_only=False) is True
    assert fc.attempts == 3


@pytest.mark.parametrize(
    ("source", "subtable", "expected"),
    [
        ({"service_id": "AbC-123"}, None, "logs_abc_123"),
        ({"service_id": "FastlyService123", "name": "Customer"}, None, "logs_fastlyservice123"),
        ({"name": "svc.two"}, "client_vitals", "logs_svc_two__client_vitals"),
        ({"service_id": 'x"; DROP TABLE lake.logs;--'}, None, "logs_x___drop_table_lake_logs"),
    ],
)
def test_ducklake_table_name_is_per_service_and_sanitized(source, subtable, expected):
    assert (ducklake_table_name(source, subtable) if subtable else ducklake_table_name(source)) == expected


def test_commit_tombstone_idempotency_and_schema_evolution(tmp_path):
    src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    p1 = str(_write_buffer_parquet(src, "batch_a.parquet", rows=3, ts=ts, source_file="s3://b/raw/a.gz"))

    res = _commit_buffer_impl(src)
    assert res["files_committed"] == 1 and res["rows_committed"] == 3 and _lake_row_count(src) == 3
    assert os.path.exists(p1) and buffer_files(src) == []

    buf_dir = os.path.dirname(p1)
    for f in os.listdir(buf_dir):
        if ".consumed-" in f:
            os.remove(os.path.join(buf_dir, f))
    assert _commit_buffer_impl(src)["files_committed"] == 1 and _lake_row_count(src) == 3

    _write_buffer_parquet(
        src, "batch_b.parquet", rows=2, ts=ts, source_file="s3://b/raw/b.gz", extra={"custom_hdr": "v1"}
    )
    assert _commit_buffer_impl(src)["rows_committed"] == 2 and _lake_row_count(src) == 5

    assert ducklake_table_exists(src) is True
    info = get_table_info(src)
    assert "error" not in info and info["snapshots"] >= 2 and info["data_files"] >= 1
    assert len(get_snapshot_calendar(src)) >= 1


def test_commit_failure_rolls_back_and_leaves_buffer_untombstoned(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"dlfail{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    p = str(_write_buffer_parquet(src, "batch_fail.parquet", rows=2, ts=ts, source_file="s3://b/raw/f.gz"))

    def _boom(q: str):
        if "INSERT INTO" in q:
            raise RuntimeError("simulated insert crash")

    _proxy_commit_connection(monkeypatch, _boom)
    res = _commit_buffer_impl(src)
    assert res["files_committed"] == 0 and res["rows_committed"] == 0 and buffer_files(src) == [p]


def test_rollup_time_series_reads_from_committed_ducklake(tmp_path):
    src = _make_source(tmp_path, f"dlrollup{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 5, tzinfo=UTC)
    _write_buffer_parquet(
        src,
        "rollup_batch.parquet",
        rows=4,
        ts=ts,
        source_file="s3://b/raw/r.gz",
        extra={"status": 200, "bytes": 1000, "time_total_us": 5000, "cache_status": "HIT"},
    )
    _commit_buffer_impl(src)
    assert build_time_series_bundles(src["name"], src, ["2026-08-30-12"]) == 1


# ── Local Compaction Tests ──


def _write_parquet(path: str, rows: int, ts_start: int = 0, rid_start: int | None = None) -> None:
    cols = {
        "timestamp": pa.array(range(ts_start, ts_start + rows), type=pa.int64()),
        "ip": pa.array([f"10.0.0.{i % 255}" for i in range(rows)]),
        "status": pa.array([200 + (i % 5) for i in range(rows)], type=pa.int32()),
    }
    if rid_start is not None:
        cols["rid"] = pa.array([f"r{rid_start + i}" for i in range(rows)])
    pq.write_table(pa.table(cols), path, compression="zstd")


def _write_parquet_with_explicit_rids(path: str, rids: list[str | None], ts_offsets: list[int]) -> None:
    assert len(rids) == len(ts_offsets)
    cols = {
        "timestamp": pa.array(ts_offsets, type=pa.int64()),
        "ip": pa.array([f"10.0.0.{i}" for i in range(len(rids))]),
        "status": pa.array([200] * len(rids), type=pa.int32()),
        "rid": pa.array(rids, type=pa.string()),
    }
    pq.write_table(pa.table(cols), path, compression="zstd")


_NAME = "test-svc-local-compact"


@pytest.fixture
def patched_cache_dir(tmp_path, monkeypatch):
    src = make_source(tmp_path / "cache" / _NAME, _NAME, cache_key="_test_cache_root", mkdirs=("data",))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda source: source["_test_cache_root"])
    monkeypatch.setattr("backend.core.storage._DAILY_TIER_AGE_DAYS", 365)
    monkeypatch.setattr("backend.core.storage._WEEKLY_TIER_AGE_DAYS", 365)
    return src


def test_skips_partitions_below_threshold(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-00")
    os.makedirs(part)
    _write_parquet(os.path.join(part, "f0.parquet"), rows=10, ts_start=0)
    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["partitions_scanned"] == 0 and result["partitions_compacted"] == 0
    assert len([f for f in os.listdir(part) if f.endswith(".parquet")]) == 1


def test_dedup_removes_cross_file_duplicate_rids(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-02")
    os.makedirs(part)
    _write_parquet(os.path.join(part, "a.parquet"), rows=10, ts_start=0, rid_start=1)
    _write_parquet(os.path.join(part, "b.parquet"), rows=10, ts_start=10, rid_start=6)

    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["partitions_compacted"] == 1
    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1
    merged_path = os.path.join(part, remaining[0])
    with closing(duckdb.connect(":memory:")) as con:
        n_rows, n_uniq = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT rid) FROM read_parquet('{merged_path}')"
        ).fetchone()
    assert n_rows == 15 and n_uniq == 15


def test_merges_partitions_above_threshold(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-01")
    os.makedirs(part)
    for i in range(5):
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10, ts_start=i * 10)
    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["partitions_compacted"] == 1 and result["files_merged"] == 5 and result["files_removed"] == 5
    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1 and remaining[0].startswith("compacted_")


def test_row_count_preserved_across_merge(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-02")
    os.makedirs(part)
    total_rows = 0
    for i in range(4):
        rows = (i + 1) * 10
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=rows, ts_start=i * 100)
        total_rows += rows
    lc.compact_local_partitions(patched_cache_dir)
    merged = [f for f in os.listdir(part) if f.endswith(".parquet")][0]
    merged_path = os.path.join(part, merged)
    with closing(duckdb.connect(":memory:")) as con:
        actual = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged_path}')").fetchone()[0]
    assert actual == total_rows


def test_dry_run_reports_without_writing(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-03")
    os.makedirs(part)
    for i in range(5):
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10, ts_start=i * 10)
    result = lc.compact_local_partitions(patched_cache_dir, dry_run=True)
    assert result["dry_run"] is True and result["partitions_compacted"] == 1 and result["files_merged"] == 5
    assert sorted(os.listdir(part)) == [f"f{i}.parquet" for i in range(5)]


def test_empty_cache_dir_is_a_noop(tmp_path, monkeypatch):
    src = {"name": "empty-svc", "_test_cache_root": str(tmp_path / "cache" / "empty-svc")}
    os.makedirs(src["_test_cache_root"])
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: s["_test_cache_root"])
    result = lc.compact_local_partitions(src)
    assert result["partitions_scanned"] == 0 and result["errors"] == []


def test_active_hour_is_never_compacted(patched_cache_dir):
    active = datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", active)
    os.makedirs(part)
    for i in range(5):
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10, ts_start=i * 10)
    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["active_hour_skipped"] is True and result["partitions_compacted"] == 0
    assert len([f for f in os.listdir(part) if f.endswith(".parquet")]) == 5


def test_stale_tmp_files_get_cleaned(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-15-00")
    os.makedirs(part)
    _write_parquet(os.path.join(part, "real.parquet"), rows=10)
    stale = os.path.join(part, ".tmp_compacted_dead.parquet")
    Path(stale).write_bytes(b"garbage")
    result = lc.compact_local_partitions(patched_cache_dir)
    assert (
        result["stale_tmp_removed"] == 1
        and not os.path.exists(stale)
        and os.path.exists(os.path.join(part, "real.parquet"))
    )


def test_partition_above_size_ceiling_is_skipped(patched_cache_dir, monkeypatch):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-04")
    os.makedirs(part)
    for i in range(5):
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10)
    monkeypatch.setattr("backend.core.storage._MAX_PARTITION_BYTES", 100)
    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["partitions_compacted"] == 0 and len([f for f in os.listdir(part) if f.endswith(".parquet")]) == 5


def test_daily_tier_rolls_up_old_partitions(patched_cache_dir, monkeypatch):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    monkeypatch.setattr("backend.core.storage._DAILY_TIER_AGE_DAYS", 0)
    day = "2026-05-15"
    for hh in ("00", "01", "02"):
        part = os.path.join(data_dir, f"timestamp_hour={day}-{hh}")
        os.makedirs(part)
        _write_parquet(os.path.join(part, "f0.parquet"), rows=10)

    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["daily_rollups"] == 1
    daily_files = [f for f in os.listdir(os.path.join(data_dir, "daily")) if f.endswith(".parquet")]
    assert len(daily_files) == 1 and daily_files[0].startswith(f"daily_{day}_")
    for hh in ("00", "01", "02"):
        assert not os.path.isdir(os.path.join(data_dir, f"timestamp_hour={day}-{hh}"))


def test_weekly_tier_rolls_up_old_daily_files(patched_cache_dir, monkeypatch):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    daily_dir = os.path.join(data_dir, "daily")
    os.makedirs(daily_dir)
    monkeypatch.setattr("backend.core.storage._WEEKLY_TIER_AGE_DAYS", 0)
    for day in ("2026-05-04", "2026-05-05", "2026-05-06"):
        _write_parquet(os.path.join(daily_dir, f"daily_{day}_abc12345.parquet"), rows=10)

    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["weekly_rollups"] == 1
    weekly_files = [f for f in os.listdir(os.path.join(data_dir, "weekly")) if f.endswith(".parquet")]
    assert len(weekly_files) == 1 and weekly_files[0].startswith("weekly_2026-W19_")
    assert not [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]


def test_weekly_tier_skips_single_day_weeks(patched_cache_dir, monkeypatch):
    cache_root = patched_cache_dir["_test_cache_root"]
    daily_dir = os.path.join(cache_root, "data", "daily")
    os.makedirs(daily_dir)
    monkeypatch.setattr("backend.core.storage._WEEKLY_TIER_AGE_DAYS", 0)
    _write_parquet(os.path.join(daily_dir, "daily_2026-05-04_deadbeef.parquet"), rows=10)

    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["weekly_rollups"] == 0
    weekly_dir = os.path.join(cache_root, "data", "weekly")
    assert not os.path.isdir(weekly_dir) or not os.listdir(weekly_dir)


def test_compaction_stats_snapshot(patched_cache_dir):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    p1 = os.path.join(data_dir, "timestamp_hour=2026-05-20-00")
    os.makedirs(p1)
    for i in range(5):
        _write_parquet(os.path.join(p1, f"f{i}.parquet"), rows=10)
    p2 = os.path.join(data_dir, "timestamp_hour=2026-05-20-01")
    os.makedirs(p2)
    _write_parquet(os.path.join(p2, "f0.parquet"), rows=10)

    s = lc.compaction_stats(patched_cache_dir)
    assert s["total_files"] == 6 and s["partitions"] == 2
    assert s["partitions_above_3"] == 1 and s["partitions_above_10"] == 0 and s["avg_files_per_partition"] == 3.0


def test_daily_tier_bin_packing_splits_files(patched_cache_dir, monkeypatch):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    monkeypatch.setattr("backend.core.storage._DAILY_TIER_AGE_DAYS", 0)
    day = "2026-05-15"
    paths = []
    for hh in ("00", "01", "02"):
        part = os.path.join(data_dir, f"timestamp_hour={day}-{hh}")
        os.makedirs(part)
        p = os.path.join(part, "f0.parquet")
        _write_parquet(p, rows=10)
        paths.append(p)

    monkeypatch.setattr("backend.core.storage._MAX_PARTITION_BYTES", os.path.getsize(paths[0]) + 50)
    lc.compact_local_partitions(patched_cache_dir)
    daily_dir = os.path.join(data_dir, "daily")
    daily_files = sorted([f for f in os.listdir(daily_dir) if f.endswith(".parquet")])
    assert len(daily_files) == 3 and all(f.startswith(f"daily_{day}_") for f in daily_files)
    with closing(duckdb.connect(":memory:")) as con:
        assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{daily_dir}/*.parquet')").fetchone()[0] == 30
    for hh in ("00", "01", "02"):
        assert not os.path.isdir(os.path.join(data_dir, f"timestamp_hour={day}-{hh}"))


def test_weekly_tier_bin_packing_splits_files(patched_cache_dir, monkeypatch):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    daily_dir = os.path.join(data_dir, "daily")
    os.makedirs(daily_dir)
    monkeypatch.setattr("backend.core.storage._WEEKLY_TIER_AGE_DAYS", 0)
    paths = []
    for day in ("2026-05-04", "2026-05-05", "2026-05-06"):
        p = os.path.join(daily_dir, f"daily_{day}_abc12345.parquet")
        _write_parquet(p, rows=10)
        paths.append(p)

    monkeypatch.setattr("backend.core.storage._MAX_PARTITION_BYTES", os.path.getsize(paths[0]) + 50)
    lc.compact_local_partitions(patched_cache_dir)
    weekly_dir = os.path.join(data_dir, "weekly")
    weekly_files = sorted([f for f in os.listdir(weekly_dir) if f.endswith(".parquet")])
    assert len(weekly_files) == 3 and all(f.startswith("weekly_2026-W19_") for f in weekly_files)
    with closing(duckdb.connect(":memory:")) as con:
        assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{weekly_dir}/*.parquet')").fetchone()[0] == 30
    assert not [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]


def test_daily_tier_migrates_single_file_bins_and_removes_dir(patched_cache_dir, monkeypatch):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    monkeypatch.setattr("backend.core.storage._DAILY_TIER_AGE_DAYS", 0)
    day = "2026-05-15"
    part = os.path.join(data_dir, f"timestamp_hour={day}-00")
    os.makedirs(part)
    _write_parquet(os.path.join(part, "single_file.parquet"), rows=10)

    lc.compact_local_partitions(patched_cache_dir)
    daily_files = [f for f in os.listdir(os.path.join(data_dir, "daily")) if f.endswith(".parquet")]
    assert len(daily_files) == 1 and daily_files[0].startswith(f"daily_{day}_") and not os.path.isdir(part)


def test_null_rid_rows_pass_through_unchanged(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-03")
    os.makedirs(part)
    _write_parquet_with_explicit_rids(
        os.path.join(part, "a.parquet"), rids=[None, None, "shared-1"], ts_offsets=[0, 1, 2]
    )
    _write_parquet_with_explicit_rids(os.path.join(part, "b.parquet"), rids=[None, "shared-1"], ts_offsets=[3, 4])

    assert lc.compact_local_partitions(patched_cache_dir)["partitions_compacted"] == 1
    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1
    merged = os.path.join(part, remaining[0])
    with closing(duckdb.connect(":memory:")) as con:
        n_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}')").fetchone()[0]
        n_null = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}') WHERE rid IS NULL").fetchone()[0]
        n_non_null = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}') WHERE rid IS NOT NULL").fetchone()[0]
    assert n_null == 3 and n_non_null == 1 and n_total == 4


def test_dedup_keeps_earliest_timestamp_for_same_rid(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-04")
    os.makedirs(part)
    _write_parquet_with_explicit_rids(os.path.join(part, "a.parquet"), rids=["shared"], ts_offsets=[100])
    _write_parquet_with_explicit_rids(os.path.join(part, "b.parquet"), rids=["shared"], ts_offsets=[50])
    _write_parquet_with_explicit_rids(os.path.join(part, "c.parquet"), rids=["shared"], ts_offsets=[200])

    lc.compact_local_partitions(patched_cache_dir)
    merged = os.path.join(part, [f for f in os.listdir(part) if f.endswith(".parquet")][0])
    with closing(duckdb.connect(":memory:")) as con:
        rows = con.execute(f"SELECT timestamp FROM read_parquet('{merged}') WHERE rid = 'shared'").fetchall()
    assert rows == [(50,)]


def test_schema_without_rid_column_falls_back_to_plain_select(patched_cache_dir):
    part = os.path.join(patched_cache_dir["_test_cache_root"], "data", "timestamp_hour=2026-05-30-05")
    os.makedirs(part)
    _write_parquet(os.path.join(part, "a.parquet"), rows=5, ts_start=0)
    _write_parquet(os.path.join(part, "b.parquet"), rows=5, ts_start=10)

    assert lc.compact_local_partitions(patched_cache_dir)["partitions_compacted"] == 1
    merged = os.path.join(part, [f for f in os.listdir(part) if f.endswith(".parquet")][0])
    with closing(duckdb.connect(":memory:")) as con:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}')").fetchone()[0]
        cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{merged}')").fetchall()}
    assert n == 10 and "rid" not in cols


def test_compact_without_ip_column(tmp_path):
    file1, file2 = os.path.join(tmp_path, "file1.parquet"), os.path.join(tmp_path, "file2.parquet")
    with closing(duckdb.connect(":memory:")) as con:
        con.execute(
            "CREATE TABLE t1 (timestamp TIMESTAMP, value INTEGER); INSERT INTO t1 VALUES ('2026-08-19 12:00:00', 42)"
        )
        con.execute(f"COPY t1 TO '{file1}' (FORMAT PARQUET); DROP TABLE t1")
        con.execute(
            "CREATE TABLE t2 (timestamp TIMESTAMP, value INTEGER); INSERT INTO t2 VALUES ('2026-08-19 12:05:00', 100)"
        )
        con.execute(f"COPY t2 TO '{file2}' (FORMAT PARQUET)")

    parquets = ["file1.parquet", "file2.parquet"]
    out_dir = str(tmp_path)
    res = _compact_single_partition(out_dir, parquets)
    assert res is not None and res["files_removed"] == 2 and len(res["removed_basenames"]) == 2

    merged_files = [f for f in os.listdir(out_dir) if f.endswith(".parquet") and f not in parquets]
    assert len(merged_files) == 1
    with closing(duckdb.connect(":memory:")) as con:
        rows = con.execute(
            f"SELECT * FROM read_parquet('{os.path.join(out_dir, merged_files[0])}') ORDER BY timestamp"
        ).fetchall()
    assert len(rows) == 2 and rows[0][1] == 42 and rows[1][1] == 100


def test_build_merge_select_sql_no_rid_plain_select():
    sql = lc._build_merge_select_sql("'a.parquet', 'b.parquet'", [], has_rid=False)
    assert "ROW_NUMBER" not in sql and "UNION ALL" not in sql and "read_parquet" in sql


def test_build_merge_select_sql_no_rid_with_exclude_cols():
    sql = lc._build_merge_select_sql("'a.parquet'", ["timestamp_hour", "dt"], has_rid=False)
    assert "EXCLUDE (timestamp_hour, dt)" in sql


def test_build_merge_select_sql_with_rid_emits_dedup_window():
    sql = lc._build_merge_select_sql("'a.parquet'", ["timestamp_hour"], has_rid=True)
    assert "ROW_NUMBER() OVER (PARTITION BY rid" in sql and "UNION ALL BY NAME" in sql
    assert "WHERE rid IS NULL" in sql and "_dup_rn" in sql and "WHERE _dup_rn = 1" in sql


def test_build_merge_select_sql_with_natural_keys_vitals():
    keys = ("timestamp", "metric_name", "pathname", "cid", "req_id")
    sql = lc._build_merge_select_sql("'a.parquet'", ["timestamp_hour"], has_rid=False, natural_keys=keys)
    assert (
        'ROW_NUMBER() OVER (PARTITION BY "timestamp", "metric_name", "pathname", "cid", "req_id" ORDER BY timestamp) AS _dup_rn'
        in sql
    )
    assert "WHERE _dup_rn = 1" in sql and "_dup_rn" in sql and "UNION ALL" not in sql


def test_build_merge_select_sql_with_natural_keys_errors():
    keys = ("timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id")
    sql = lc._build_merge_select_sql("'a.parquet'", [], has_rid=False, natural_keys=keys)
    assert 'PARTITION BY "timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id"' in sql
    assert "WHERE _dup_rn = 1" in sql and "EXCLUDE (_dup_rn)" in sql


def test_detect_natural_keys():
    vitals_cols = {"timestamp", "metric_name", "pathname", "cid", "req_id", "metric_value", "browser"}
    assert lc._detect_natural_keys(vitals_cols) == ("timestamp", "metric_name", "pathname", "cid", "req_id")
    errors_cols = {"timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id", "browser"}
    assert lc._detect_natural_keys(errors_cols) == (
        "timestamp",
        "error_message",
        "error_file",
        "error_line",
        "pathname",
        "cid",
        "req_id",
    )
    assert lc._detect_natural_keys({"timestamp", "rid", "service_id", "status"}) is None


def test_bin_pack_files_empty_returns_empty(tmp_path):
    assert lc._bin_pack_files([], 1024) == []


def test_bin_pack_files_single_small_file(tmp_path):
    p = tmp_path / "a.parquet"
    p.write_bytes(b"x" * 100)
    assert lc._bin_pack_files([str(p)], 1024) == [[str(p)]]


def test_bin_pack_files_packs_under_cap(tmp_path):
    files = []
    for name, size in [("a.parquet", 200), ("b.parquet", 200), ("c.parquet", 200)]:
        p = tmp_path / name
        p.write_bytes(b"x" * size)
        files.append(str(p))
    assert lc._bin_pack_files(files, 1024) == [files]


def test_bin_pack_files_starts_new_bin_when_cap_exceeded(tmp_path):
    files = []
    for name, size in [("a.parquet", 400), ("b.parquet", 400), ("c.parquet", 400)]:
        p = tmp_path / name
        p.write_bytes(b"x" * size)
        files.append(str(p))
    bins = lc._bin_pack_files(files, 1000)
    assert len(bins) == 2 and bins[0] == files[:2] and bins[1] == [files[2]]


def test_bin_pack_files_huge_single_file_gets_own_bin(tmp_path):
    files = []
    for name, size in [("a.parquet", 100), ("huge.parquet", 5000), ("b.parquet", 100)]:
        p = tmp_path / name
        p.write_bytes(b"x" * size)
        files.append(str(p))
    bins = lc._bin_pack_files(files, 1000)
    assert [str(tmp_path / "huge.parquet")] in bins
    assert sorted(p for b in bins for p in b) == sorted(files)


def test_bin_pack_files_skips_files_that_oserror_on_getsize(tmp_path):
    real = tmp_path / "real.parquet"
    real.write_bytes(b"x" * 100)
    assert lc._bin_pack_files([str(tmp_path / "ghost.parquet"), str(real)], 1024) == [[str(real)]]


def test_cleanup_stale_tmp_removes_both_naming_conventions(tmp_path):
    (tmp_path / "a.parquet.tmp").write_bytes(b"")
    (tmp_path / ".tmp_b.parquet").write_bytes(b"")
    (tmp_path / "keep.parquet").write_bytes(b"keep")
    nested = tmp_path / "sub"
    nested.mkdir()
    (nested / "c.parquet.tmp").write_bytes(b"")

    assert lc._cleanup_stale_tmp(str(tmp_path)) == 3
    assert (tmp_path / "keep.parquet").exists()
    assert (
        not (tmp_path / "a.parquet.tmp").exists()
        and not (tmp_path / ".tmp_b.parquet").exists()
        and not (nested / "c.parquet.tmp").exists()
    )


def test_cleanup_stale_tmp_swallows_oserror(tmp_path):
    (tmp_path / "a.parquet.tmp").write_bytes(b"")
    (tmp_path / "b.parquet.tmp").write_bytes(b"")
    real_remove = os.remove
    call_count = {"n": 0}

    def _maybe_fail(p):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise OSError("simulated")
        return real_remove(p)

    with patch("os.remove", side_effect=_maybe_fail):
        assert lc._cleanup_stale_tmp(str(tmp_path)) == 1


def test_compact_local_partitions_returns_skeleton_when_data_dir_missing(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = lc.compact_local_partitions(src)
    assert out["partitions_scanned"] == 0 and out["files_merged"] == 0 and out["errors"] == []
    assert out["duration_ms"] >= 0 and out["dry_run"] is False


def test_compact_local_partitions_active_hour_flag_set(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    (data_dir / datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")).mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        assert lc.compact_local_partitions(src)["active_hour_skipped"] is True


def test_compact_local_partitions_skips_non_directory_entries(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    (data_dir / "stray.txt").write_bytes(b"x")
    (data_dir / "empty_part").mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = lc.compact_local_partitions(src)
    assert out["errors"] == [] and out["partitions_scanned"] == 0


# ── Merged from tests/core/test_metadata_db_crud.py ──

import contextlib
import gc
import logging
import sqlite3

import pytest

import backend.core.metadata as metadata_db
import backend.core.metadata.base as pgc
import backend.core.metadata.base as reconciliation
import backend.core.metadata.usage_log as usage_log_db
import backend.core.request_telemetry as query_registry
from backend.core import duckdb as _db
from backend.core.metadata.usage_log import (
    _decode_outcome_counters,
    _normalize_outcome_counters,
    _usage_class_predicate,
    record_ingest_object_outcome,
)
from backend.core.rum_ingest import ingest_rum_logs
from backend.utils.date_utils import iso_z


@pytest.fixture
def sid() -> str:
    return "test-svc-md"


@pytest.fixture()
def _reset_pg_state():
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()
    yield
    pgc.close_all_pg_connections()
    pgc.reset_pg_pool_for_tests()


# ── Shared test helpers ─────────────────────────────────────────────────────


def _insert_raw_ingested_file(
    sid: str,
    file_name: str,
    *,
    row_count: int = 1,
    file_size_bytes: int = 1,
    ingested_at: str | None = None,
    null_file_date: bool = False,
) -> None:
    con = metadata_db.get_con(sid)
    if null_file_date:
        sql = "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) VALUES (?, ?, ?, ?, NULL)"
        params: tuple = (file_name, sid, row_count, file_size_bytes)
    elif ingested_at is not None:
        sql = "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) VALUES (?, ?, ?, ?, ?)"
        params = (file_name, sid, ingested_at, row_count, file_size_bytes)
    else:
        sql = "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes) VALUES (?, ?, ?, ?)"
        params = (file_name, sid, row_count, file_size_bytes)
    con.execute(sql, params)
    con.commit()


def _seed_ledger_row(
    sid: str, object_key: str, originating_run_id: int, originating_task: str = "log_discovery"
) -> None:
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, originating_task, originating_run_id) VALUES (?, ?, 'discovered', ?, ?)",
        (sid, object_key, originating_task, originating_run_id),
    )
    con.commit()


def _make_outcome_counters(**overrides: int) -> dict[str, int]:
    keys = (
        "valid_records",
        "malformed_records",
        "corrupt_containers",
        "quarantine_capture_failures",
        "source_delete_failures",
        "cap_evictions",
        "objects_processed",
        "objects_successful",
        "objects_partial",
        "objects_failed",
    )
    return {**dict.fromkeys(keys, 0), **overrides}


@contextlib.contextmanager
def _exploding_executemany(con):
    real_executemany = con.executemany

    def _explode(*_a, **_kw):
        raise RuntimeError("simulated insert failure")

    con.executemany = _explode  # type: ignore[method-assign]
    try:
        yield
    finally:
        con.executemany = real_executemany  # type: ignore[method-assign]


# ── audit_logs ──────────────────────────────────────────────────────────────


def test_audit_logs_crud_and_export(sid):
    metadata_db.record_audit(sid, "alert_created", {"alert_id": "a1", "name": "5xx"}, actor="dmichael")
    entries = metadata_db.list_audit(sid)
    assert len(entries) == 1
    assert (entries[0]["event_type"], entries[0]["actor"]) == ("alert_created", "dmichael")
    assert json.loads(entries[0]["details"]) == {"alert_id": "a1", "name": "5xx"}
    assert metadata_db.list_audit(sid, since="2099-01-01T00:00:00Z") == []
    assert len(metadata_db.list_audit(sid, since="2000-01-01T00:00:00Z")) == 1
    assert metadata_db.list_audit(sid) == metadata_db.export_audit(sid)

    for i in range(5):
        metadata_db.record_audit(sid, f"event_{i}", {}, actor="x")
    assert len(metadata_db.list_audit(sid, limit=2)) == 2


# ── ingested_files & dedup cache ────────────────────────────────────────────


def test_ingested_files_insert_upsert_and_ordering(sid):
    metadata_db.insert_ingested_files(sid, [("a.gz", 100, 5000), ("b.gz", 200, 8000)])
    assert metadata_db.get_ingested_filenames(sid) == {"a.gz", "b.gz"}

    metadata_db.insert_ingested_files(sid, [("a.gz", 999, 99999)])
    by_name = {f["file_name"]: f for f in metadata_db.list_ingested_files(sid)}
    assert (by_name["a.gz"]["row_count"], by_name["a.gz"]["file_size_bytes"]) == (999, 99999)

    sid2 = f"{sid}-order"
    _insert_raw_ingested_file(sid2, "first.gz", row_count=1, file_size_bytes=100, ingested_at="2026-01-01T00:00:00Z")
    _insert_raw_ingested_file(sid2, "second.gz", row_count=2, file_size_bytes=200, ingested_at="2026-01-02T00:00:00Z")
    assert [f["file_name"] for f in metadata_db.list_ingested_files(sid2)] == ["second.gz", "first.gz"]


def test_get_reclaimable_strand_filenames(sid):
    metadata_db.insert_ingested_files(
        sid, [("durable.gz", 100, 5000), ("marker.gz", 0, 1200), ("legacy.gz", 100, 8000)]
    )
    con = metadata_db.get_con(sid)
    con.execute(
        "UPDATE ingested_files SET ingested_at = ? WHERE file_name = ? AND source_name = ?",
        ("2020-01-01 00:00:00", "legacy.gz", sid),
    )
    con.commit()

    cands = {"durable.gz", "marker.gz", "legacy.gz"}
    assert metadata_db.get_reclaimable_strand_filenames(sid, cands, "2000-01-01 00:00:00") == {
        "durable.gz",
        "legacy.gz",
    }
    assert metadata_db.get_reclaimable_strand_filenames(sid, cands, "2025-01-01 00:00:00") == {"durable.gz"}
    assert metadata_db.get_reclaimable_strand_filenames(sid, {"durable.gz", "marker.gz"}, "2000-01-01 00:00:00") == {
        "durable.gz"
    }
    assert metadata_db.get_reclaimable_strand_filenames(sid, set(), "2000-01-01 00:00:00") == set()


def test_get_node_count_avg_variants(sid):
    assert metadata_db.get_node_count_avg(sid) is None
    _insert_raw_ingested_file(sid, "short-T.gz", null_file_date=True)
    _insert_raw_ingested_file(sid, "lowercase-only.gz", null_file_date=True)
    assert metadata_db.get_node_count_avg(sid) is None

    sid_a = f"{sid}-a"
    metadata_db.insert_ingested_files(
        sid_a,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-aaa.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-bbb.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:30.000-ccc.log.gz", 1, 1),
        ],
    )
    assert metadata_db.get_node_count_avg(sid_a) == 1.5

    sid_b = f"{sid}-b"
    metadata_db.insert_ingested_files(
        sid_b,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-a.log.gz", 1, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 1, 1),
            *[(f"s3://b/raw/2026-05-15/23/2026-05-15T23:31:00.000-{ch}.log.gz", 1, 1) for ch in ("c", "d", "e", "f")],
        ],
    )
    for minute, suffix in (("32", "x"), ("33", "y")):
        _insert_raw_ingested_file(
            sid_b,
            f"s3://b/raw/2026-05-15/23/2026-05-15T23:{minute}:00.000-legacy-{suffix}.log.gz",
            null_file_date=True,
        )
    assert metadata_db.get_node_count_avg(sid_b) == 2.0


def test_get_log_accounting_counts_arms(sid):
    sid_iso = f"{sid}-iso"
    _insert_raw_ingested_file(
        sid_iso,
        "s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz",
        row_count=100,
        ingested_at="2026-05-15T23:00:05",
    )
    _insert_raw_ingested_file(
        sid_iso,
        "s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz",
        row_count=250,
        ingested_at="2026-05-15T23:30:05",
    )
    assert metadata_db.get_log_accounting_counts(
        sid_iso, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    ) == {"2026-05-15T23": (350, 2)}

    sid_fd = f"{sid}-fd"
    metadata_db.insert_ingested_files(
        sid_fd,
        [
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:00:00.000-a.log.gz", 100, 1),
            ("s3://b/raw/2026-05-15/23/2026-05-15T23:30:00.000-b.log.gz", 250, 1),
        ],
    )
    fd_rows = (
        metadata_db.get_con(sid_fd)
        .execute("SELECT file_name, file_date FROM ingested_files WHERE source_name = ?", (sid_fd,))
        .fetchall()
    )
    assert all(str(r["file_date"]) == "2026-05-15" for r in fd_rows)
    assert metadata_db.get_log_accounting_counts(
        sid_fd, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    ) == {"2026-05-15T23": (350, 2)}

    sid_fb = f"{sid}-fb"
    _insert_raw_ingested_file(sid_fb, "raw/legacy.gz", row_count=42, ingested_at="2026-05-15T23:00:00")
    assert metadata_db.get_log_accounting_counts(
        sid_fb, "2026-05-15T22:00:00", "2026-05-16T00:00:00", 13, "2026-05-15T23", "2026-05-15T23"
    ) == {"2026-05-15T23": (42, 1)}


def test_ingested_filenames_dedup_cache_lifecycle(sid):
    metadata_db.insert_ingested_files(sid, [("a.gz", 1, 100), ("b.gz", 2, 200)])
    metadata_db._clear_ingested_filenames_cache(sid)
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}

    con = metadata_db.get_con(sid)
    con.execute("DELETE FROM ingested_files WHERE source_name = ?", (sid,))
    con.commit()
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz"}

    metadata_db.insert_ingested_files(sid, [("c.gz", 2, 200)])
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"a.gz", "b.gz", "c.gz"}

    snapshot = metadata_db.get_ingested_filenames(sid, limit=200)
    snapshot.add("mutated.gz")
    assert "mutated.gz" not in metadata_db.get_ingested_filenames(sid, limit=200)

    metadata_db._clear_ingested_filenames_cache(sid)
    metadata_db.insert_ingested_files(sid, [("d.gz", 1, 100)])
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"c.gz", "d.gz"}

    _insert_raw_ingested_file(sid, "sideloaded.gz", row_count=1, file_size_bytes=100)
    assert metadata_db.get_ingested_filenames(sid) == {"c.gz", "d.gz", "sideloaded.gz"}
    assert metadata_db.get_ingested_filenames(sid, limit=200) == {"c.gz", "d.gz", "sideloaded.gz"}

    assert (sid, "logs") in metadata_db._ingested_filenames_cache
    metadata_db.teardown(sid)
    assert (sid, "logs") not in metadata_db._ingested_filenames_cache


# ── cron_runs: start + log + busy + summary ────────────────────────────────


def test_cron_run_start_busy_and_orphan_reap(sid):
    assert metadata_db.cron_busy(sid) is False
    run_id = metadata_db.start_cron_run(sid, "sync")
    assert isinstance(run_id, int) and run_id > 0 and metadata_db.cron_busy(sid) is True
    with pytest.raises(RuntimeError, match="already running"):
        metadata_db.start_cron_run(sid, "sync")

    assert metadata_db.start_cron_run("cronseam-b", "sync") > 0

    metadata_db.log_cron_run(
        sid, "sync", duration_s=12.5, status="success", files_downloaded=10, rows_ingested=1000, run_id=run_id
    )
    assert metadata_db.cron_busy(sid) is False
    row = (
        metadata_db.get_con(sid)
        .execute("SELECT status, duration_s, rows_ingested FROM cron_runs WHERE id = ?", (run_id,))
        .fetchone()
    )
    assert (row["status"], row["duration_s"], row["rows_ingested"]) == ("success", 12.5, 1000)

    old_ts = (datetime.now(UTC) - timedelta(hours=2)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, parquet_keys) VALUES (?, ?, 'running', '[]')",
        ("sync", old_ts),
    )
    con.commit()
    assert isinstance(metadata_db.start_cron_run(sid, "sync"), int)

    metadata_db.log_cron_run(sid, "manual", duration_s=1.0, status="success", files_downloaded=1)
    rows = con.execute("SELECT task, status FROM cron_runs WHERE task = 'manual'").fetchall()
    assert len(rows) == 1 and rows[0]["status"] == "success"


def test_log_cron_run_persists_outcome_counters_as_json(sid):
    run_id = metadata_db.start_cron_run(sid, "log_discovery")
    counters = _make_outcome_counters(
        valid_records=3,
        malformed_records=1,
        cap_evictions=2,
        objects_processed=2,
        objects_successful=1,
        objects_partial=1,
    )
    metadata_db.log_cron_run(
        sid,
        "log_discovery",
        duration_s=1.0,
        status="error",
        files_downloaded=2,
        rows_ingested=3,
        corrupt_rows=1,
        outcome_counters=counters,
        run_id=run_id,
    )
    con = metadata_db.get_con(sid)
    columns = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'cron_runs'"
    ).fetchall()
    assert "outcome_counters" in {row[0] for row in columns}
    row = con.execute(
        "SELECT status, files_downloaded, rows_ingested, corrupt_rows, outcome_counters FROM cron_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row[0:4] == ("error", 2, 3, 1) and json.loads(row[4]) == counters


def test_record_ingest_object_outcome_and_validation(sid):
    run_id = metadata_db.start_cron_run(sid, "log_discovery")
    _seed_ledger_row(sid, "raw/request/object.log.gz", run_id)
    _seed_ledger_row(sid, "raw/request/other.log.gz", run_id)

    initial = _make_outcome_counters(valid_records=4, objects_processed=1, objects_successful=1)
    assert record_ingest_object_outcome(sid, "raw/request/object.log.gz", initial)
    assert record_ingest_object_outcome(sid, "raw/request/object.log.gz", initial)
    other = {**initial, "valid_records": 6}
    assert record_ingest_object_outcome(sid, "raw/request/other.log.gz", other)

    revised = {**initial, "malformed_records": 2, "objects_successful": 0, "objects_partial": 1}
    assert record_ingest_object_outcome(sid, "raw/request/object.log.gz", revised)
    expected_totals = {key: revised[key] + other[key] for key in revised}
    metadata_db.log_cron_run(sid, "log_discovery", duration_s=1.0, status="success", run_id=run_id)

    con = metadata_db.get_con(sid)
    row = con.execute(
        "SELECT status, rows_ingested, corrupt_rows, outcome_counters FROM cron_runs WHERE id = ? AND service_id = ?",
        (run_id, sid),
    ).fetchone()
    assert (row["status"], row["rows_ingested"], row["corrupt_rows"]) == ("error", 10, 2)
    assert json.loads(row["outcome_counters"]) == expected_totals
    ledger_row = con.execute(
        "SELECT outcome_counters FROM ingest_ledger WHERE service_id = ? AND object_key = ?",
        (sid, "raw/request/object.log.gz"),
    ).fetchone()
    assert json.loads(ledger_row[0]) == revised

    assert record_ingest_object_outcome("unknown-svc", "raw/request/missing.log.gz", {}) is False

    _seed_ledger_row(sid, "raw/request/missing-run.log.gz", 987654)
    with pytest.raises(RuntimeError, match="originating cron run 987654 is missing"):
        record_ingest_object_outcome(sid, "raw/request/missing-run.log.gz", {"valid_records": 1})

    rum_run_id = metadata_db.start_cron_run(sid, "rum_discovery")
    _seed_ledger_row(sid, "raw/request/wrong-task.log.gz", rum_run_id, originating_task="log_discovery")
    with pytest.raises(RuntimeError, match="origin task does not match"):
        record_ingest_object_outcome(sid, "raw/request/wrong-task.log.gz", {"valid_records": 1})

    counters = _normalize_outcome_counters({"valid_records": 2})
    assert (counters["valid_records"], counters["malformed_records"], len(counters)) == (2, 0, 10)


@pytest.mark.parametrize(
    "counters",
    [{"unexpected": 1}, {"valid_records": -1}, {"valid_records": True}, {"valid_records": 1.5}],
)
def test_normalize_outcome_counters_rejects_unknown_and_invalid_values(counters):
    with pytest.raises(ValueError):
        _normalize_outcome_counters(counters)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("{broken", "invalid outcome_counters JSON"),
        ("[]", "must be a JSON object"),
        ('{"valid_records": -1}', "invalid outcome_counters values"),
    ],
)
def test_decode_outcome_counters_rejects_malformed_persisted_state(raw, message):
    with pytest.raises(RuntimeError, match=message):
        _decode_outcome_counters(raw)


def test_latest_cron_per_task_ranking_and_exclusions(sid):
    r1 = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=1.0, status="success", run_id=r1)
    r2 = metadata_db.start_cron_run(sid, "sync")
    metadata_db.log_cron_run(sid, "sync", duration_s=2.0, status="error", run_id=r2, error_message="boom")

    r3 = metadata_db.start_cron_run(sid, "optimize")
    metadata_db.log_cron_run(sid, "optimize", duration_s=3.0, status="success", run_id=r3)
    metadata_db.start_cron_run(sid, "optimize")

    out = metadata_db.latest_cron_per_task(sid)
    assert set(out.keys()) == {"sync", "optimize"}
    assert (out["sync"]["status"], out["sync"]["error_message"]) == ("error", "boom")
    assert (out["optimize"]["status"], out["optimize"]["duration_s"]) == ("success", 3.0)

    sentinel = "Process interrupted by server restart"
    ra1 = metadata_db.start_cron_run(sid, "insights_prewarmer")
    metadata_db.log_cron_run(sid, "insights_prewarmer", duration_s=1.0, status="success", run_id=ra1)
    ra2 = metadata_db.start_cron_run(sid, "insights_prewarmer")
    metadata_db.log_cron_run(
        sid, "insights_prewarmer", duration_s=0.0, status="error", run_id=ra2, error_message=sentinel
    )
    assert (
        metadata_db.latest_cron_per_task(sid, exclude_error_messages=(sentinel,))["insights_prewarmer"]["status"]
        == "success"
    )
    assert metadata_db.latest_cron_per_task(sid)["insights_prewarmer"]["error_message"] == sentinel


# ── adaptive_stale_minutes, asn_names, sources ──────────────────────────────


def _seed_sync_run(sid: str, minutes_ago: float, *, status: str = "success", rows_ingested: int = 1) -> None:
    con = metadata_db.get_con(sid)
    ts = (datetime.now(UTC) - timedelta(minutes=minutes_ago)).isoformat(timespec="seconds").replace("+00:00", "Z")
    con.execute(
        "INSERT INTO cron_runs (task, started_at, status, rows_ingested) VALUES (?, ?, ?, ?)",
        ("sync", ts, status, rows_ingested),
    )
    con.commit()


def test_adaptive_stale_minutes_behaviors():
    con = metadata_db.get_con("stale-short")
    for i in range(3):
        _seed_sync_run("stale-short", minutes_ago=i * 40)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 30

    con.execute("DELETE FROM cron_runs")
    for i in range(15):
        _seed_sync_run("stale-wide", minutes_ago=i * 40)
        _seed_sync_run("stale-wide", minutes_ago=i * 40 + 20, rows_ingested=0)
    _seed_sync_run("stale-wide", minutes_ago=5, status="error", rows_ingested=5)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 80

    con.execute("DELETE FROM cron_runs")
    for i in range(15):
        _seed_sync_run("stale-narrow", minutes_ago=i * 5)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30) == 30

    con.execute("DELETE FROM cron_runs")
    for i in range(15):
        _seed_sync_run("stale-cap", minutes_ago=i * 10)
    _seed_sync_run("stale-cap", minutes_ago=15 * 10 + 2 * 24 * 60)
    assert metadata_db.adaptive_stale_minutes(con, default_minutes=30, max_minutes=1440) == 1440

    class _BoomConnection:
        def execute(self, *_a, **_k):
            raise sqlite3.OperationalError("no such column: files_downloaded")

    assert metadata_db.adaptive_stale_minutes(_BoomConnection(), default_minutes=30) == 30


def test_asn_names_and_sources_registry(sid):
    assert metadata_db.lookup_asn_names(sid, []) == {}
    metadata_db.upsert_asn_names(sid, {})
    metadata_db.upsert_asn_names(sid, {7922: "COMCAST", 16509: "AMAZON", 14618: "AMAZON-AES"})
    assert metadata_db.lookup_asn_names(sid, [7922, 16509, 99999]) == {7922: "COMCAST", 16509: "AMAZON"}

    metadata_db.upsert_asn_names(sid, {7922: "New Name"})
    assert metadata_db.lookup_asn_names(sid, [7922])[7922] == "New Name"
    assert set(metadata_db.asn_ints_for_search(sid, "%AMAZON%")) == {16509, 14618}
    assert 16509 in metadata_db.asn_ints_for_search(sid, "%amazon%")

    con = metadata_db.get_con(sid)
    old = (datetime.now(UTC) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.execute("INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?)", (1, "OLD", old))
    con.commit()
    assert metadata_db.lookup_asn_names(sid, [1], max_age_days=30) == {}

    metadata_db.register_source(sid, "svc1", '{"a": 1}', "logs_svc1")
    metadata_db.register_source(sid, "svc1", '{"a": 2}', "logs_svc1")
    out = con.execute("SELECT config FROM sources WHERE name = ?", ("svc1",)).fetchone()
    assert json.loads(out["config"])["a"] == 1


# ── usage_log telemetry & reconciliation ────────────────────────────────────


def _seed_usage_log_row(
    sid: str,
    timestamp: str,
    operation_class: str = "A",
    operation_type: str = "PUT_OBJECT",
    process_context: str = "cron:sync",
    bytes_count: int = 1024,
) -> None:
    con = usage_log_db.get_con(sid)
    con.execute(
        "INSERT INTO usage_log "
        "(service_id, timestamp, operation_class, operation_type, url, bytes, "
        "duration_ms, function_name, process_context, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            sid,
            timestamp,
            operation_class,
            operation_type,
            "https://x.example/k",
            bytes_count,
            5.0,
            "ingest",
            process_context,
            "OK",
        ],
    )
    con.execute(
        "INSERT INTO usage_log_hourly_summary "
        "(service_id, hour, operation_class, operation_type, count, bytes, last_updated) "
        "VALUES (?, ?, ?, ?, 1, ?, ?) "
        "ON CONFLICT (service_id, hour, operation_class, operation_type) "
        "DO UPDATE SET count = usage_log_hourly_summary.count + 1, "
        "bytes = usage_log_hourly_summary.bytes + EXCLUDED.bytes, "
        "last_updated = EXCLUDED.last_updated",
        [sid, timestamp[:13], operation_class, operation_type, bytes_count, timestamp],
    )
    con.commit()


def _query_jan_usage(sid: str, **kwargs):
    return metadata_db.get_usage_logs(
        service_id=sid, start="2026-01-01T00:00:00Z", end="2026-01-31T23:59:59Z", **kwargs
    )


def test_usage_class_predicate_and_log_usage_calls(sid, caplog):
    for usage_type, want in {
        "": ("", []),
        "CDN": ("operation_class = 'CDN'", []),
        "FOS-A": ("operation_class = 'A'", []),
        "FOS-B": ("operation_class = 'B'", []),
        "FOS": ("operation_class IN ('A', 'B')", []),
        "custom-x": ("operation_class = ?", ["custom-x"]),
    }.items():
        assert _usage_class_predicate(usage_type) == want

    metadata_db.log_usage_calls(sid, [])
    assert usage_log_db.get_con(sid).execute("SELECT count(*) FROM usage_log").fetchone()[0] == 0

    metadata_db.log_usage_calls(
        sid,
        [
            {"method": "PUT_OBJECT", "service": "FOS"},
            {"method": "GET_OBJECT", "service": "FOS"},
            {"method": "LIST_OBJECTS_V2", "service": "FOS"},
            {"method": "GET", "service": "CDN"},
        ],
    )
    classes = [
        r["operation_class"]
        for r in usage_log_db.get_con(sid).execute("SELECT operation_class FROM usage_log ORDER BY id").fetchall()
    ]
    assert classes == ["A", "B", "A", "CDN"]

    metadata_db.clear_usage_log(sid)
    cases = [
        ("/file.parquet", "HIT · duckdb httpfs", 1000),
        ("/file2.parquet", "MISS, HIT · duckdb httpfs", 1000),
        ("/file3.parquet", "MISS, MISS · duckdb httpfs", 2000),
        ("/file4.parquet", "MISS, PASS · duckdb httpfs", 2000),
        ("/file5.parquet", "PASS, PASS · duckdb httpfs", 2000),
        ("/file6.parquet", "HIT, MISS · duckdb httpfs", 2000),
    ]
    for path, details, _ in cases:
        metadata_db.log_usage_calls(
            sid,
            [
                {
                    "method": "GET",
                    "service": "CDN",
                    "path": path,
                    "status": "200",
                    "time_ms": 10.0,
                    "bytes": 1000,
                    "details": details,
                }
            ],
        )
    logs, _, _ = metadata_db.get_usage_logs(sid, "2000-01-01", "2099-01-01")
    logs.sort(key=lambda x: x["url"])
    assert [(row["url"], row["bytes"]) for row in logs] == [(p, exp) for p, _, exp in cases]

    con = usage_log_db.get_con(sid)
    with _exploding_executemany(con), caplog.at_level(logging.ERROR, logger="backend.core.metadata.usage_log"):
        metadata_db.log_usage_calls(
            sid, [{"function_name": "ingest", "operation_type": "PUT_OBJECT", "url": "/x", "status": "OK"}]
        )
    assert any("Failed to log usage calls" in r.message for r in caplog.records)


def test_get_usage_logs_one_pass_aggregates_and_filters(sid):
    rows, total, agg = _query_jan_usage(sid)
    assert (
        rows == [] and total == 0 and (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_bytes"]) == (0, 0, 0)
    )

    con = usage_log_db.get_con(sid)
    con.executemany(
        "INSERT INTO usage_log "
        "(timestamp, service_id, operation_class, operation_type, url, status, "
        "duration_ms, function_name, process_context, bytes, count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("2026-05-25T10:00:00Z", sid, "A", "PUT_OBJECT", "u1", "OK", 1.0, "fn1", None, 100, 2),
            ("2026-05-25T10:00:01Z", sid, "A", "PUT_OBJECT", "u2", "OK", 1.0, "fn1", None, 200, 3),
            ("2026-05-25T10:00:02Z", sid, "A", "POST", "u3", "OK", 1.0, "fn1", None, 50, 1),
            ("2026-05-25T10:00:03Z", sid, "B", "GET_OBJECT", "u4", "OK", 1.0, "fn1", None, 500, 5),
            ("2026-05-25T10:00:04Z", sid, "CDN", "GET", "u5", "OK", 1.0, "fn1", None, 1000, 7),
            ("2026-05-25T10:00:05Z", sid, "CDN", "GET", "u6", "OK", 1.0, "fn1", None, 2000, 4),
        ],
    )
    con.executemany(
        "INSERT INTO usage_log_hourly_summary "
        "(service_id, hour, operation_class, operation_type, count, bytes, last_updated) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (service_id, hour, operation_class, operation_type) "
        "DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, "
        "bytes = usage_log_hourly_summary.bytes + EXCLUDED.bytes, "
        "last_updated = EXCLUDED.last_updated",
        [
            (sid, "2026-05-25T10", "A", "PUT_OBJECT", 5, 300, "2026-05-25T10:00:00Z"),
            (sid, "2026-05-25T10", "A", "POST", 1, 50, "2026-05-25T10:00:00Z"),
            (sid, "2026-05-25T10", "B", "GET_OBJECT", 5, 500, "2026-05-25T10:00:00Z"),
            (sid, "2026-05-25T10", "CDN", "GET", 11, 3000, "2026-05-25T10:00:00Z"),
        ],
    )
    con.commit()

    _entries, total, agg = metadata_db.get_usage_logs(sid, "2026-05-25T00:00:00Z", "2026-05-25T23:59:59Z")
    assert total == 22
    assert (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_downloads"]) == (6, 5, 11)
    assert (agg["total_cdn_bytes"], agg["total_fos_bytes"]) == (3000, 850)
    assert agg["class_a_breakdown"] == {"PUT_OBJECT": 5, "POST": 1}
    assert agg["class_b_breakdown"] == {"GET_OBJECT": 5}


def test_get_usage_logs_query_variants(sid):
    for ts in ("2026-01-01T00:00:00Z", "2026-01-15T12:00:00Z", "2025-12-31T00:00:00Z", "2026-02-01T00:00:00Z"):
        _seed_usage_log_row(sid, ts)
    _seed_usage_log_row("other-svc", "2026-01-10T00:00:00Z")
    rows, total, _ = _query_jan_usage(sid)
    assert total == 2 and len(rows) == 2

    sid_f = f"{sid}-filters"
    _seed_usage_log_row(sid_f, "2026-01-10T00:00:00Z", operation_class="A", operation_type="OLDEST", bytes_count=100)
    _seed_usage_log_row(
        sid_f, "2026-01-10T00:01:00Z", operation_class="B", process_context="api:dashboard", bytes_count=500
    )
    _seed_usage_log_row(sid_f, "2026-01-15T00:00:00Z", operation_class="CDN", operation_type="NEWEST", bytes_count=1000)

    r_cdn, t_cdn, _ = _query_jan_usage(sid_f, usage_type="CDN")
    assert t_cdn == 1 and r_cdn[0]["operation_class"] == "CDN"
    r_fos, t_fos, _ = _query_jan_usage(sid_f, usage_type="FOS")
    assert t_fos == 2 and {r["operation_class"] for r in r_fos} == {"A", "B"}
    r_fosa, t_fosa, _ = _query_jan_usage(sid_f, usage_type="FOS-A")
    assert t_fosa == 1 and r_fosa[0]["operation_class"] == "A"
    r_ctx, t_ctx, _ = _query_jan_usage(sid_f, process_context="api")
    assert t_ctx == 1 and "api" in r_ctx[0]["process_context"]
    r_op, t_op, _ = _query_jan_usage(sid_f, operation_type="OLD")
    assert t_op == 1 and r_op[0]["operation_type"] == "OLDEST"

    r_all, _, agg = _query_jan_usage(sid_f)
    assert [r["operation_type"] for r in r_all] == ["NEWEST", "PUT_OBJECT", "OLDEST"]
    assert (agg["total_class_a"], agg["total_class_b"], agg["total_cdn_downloads"]) == (1, 1, 1)
    assert (agg["total_cdn_bytes"], agg["total_fos_bytes"]) == (1000, 600)

    r_page, t_page, _ = _query_jan_usage(sid_f, page=2, page_size=1)
    assert t_page == 3 and len(r_page) == 1

    _, _, agg_custom = _query_jan_usage(sid_f, usage_type="CUSTOM")
    assert (agg_custom["total_class_a"], agg_custom["total_class_b"]) == (0, 0)

    sid_sub = f"{sid}-subhour"
    for ts in ("2026-05-25T10:05:00Z", "2026-05-25T10:15:00Z", "2026-05-25T11:05:00Z"):
        _seed_usage_log_row(sid_sub, ts, operation_class="A")
    _, total_sub, agg_sub = metadata_db.get_usage_logs(sid_sub, "2026-05-25T10:00:00Z", "2026-05-25T10:30:00Z")
    assert total_sub == 2 and agg_sub["total_class_a"] == 2


def test_log_synthetic_usage_and_reconcile_fastly_stats(sid, caplog):
    assert metadata_db.log_synthetic_usage(sid, []) == 0
    assert metadata_db.log_synthetic_usage(sid, [{"method": "PUT"}, {"caller": "x"}]) == 0
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/b"}]) == 2
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/c"}]) == 1
    assert metadata_db.log_synthetic_usage(sid, [{"path": "/a"}, {"path": "/c"}]) == 0

    urls = sorted(
        r["url"]
        for r in usage_log_db.get_con(sid)
        .execute("SELECT url FROM usage_log WHERE function_name = 'fastly.edge' ORDER BY url")
        .fetchall()
    )
    assert urls == ["/a", "/b", "/c"]

    con = usage_log_db.get_con(sid)
    with _exploding_executemany(con), caplog.at_level(logging.ERROR, logger="backend.core.metadata.usage_log"):
        assert metadata_db.log_synthetic_usage(sid, [{"path": "/x"}, {"path": "/y"}]) == 0
    assert any("Synthetic usage log failed" in r.message for r in caplog.records)

    assert metadata_db.reconcile_fastly_stats(sid, []) == 0
    assert (
        metadata_db.reconcile_fastly_stats(
            sid, [{"class_a": 10, "class_b": 5}, {"hour_iso": "not-a-date", "class_a": 1}]
        )
        == 0
    )

    for i in range(5):
        _seed_usage_log_row(sid, f"2026-05-22T13:0{i}:00Z", operation_class="A")
    assert (
        metadata_db.reconcile_fastly_stats(sid, [{"hour_iso": "2026-05-22T13:00:00Z", "class_a": 10, "class_b": 3}])
        == 2
    )
    rows = con.execute(
        "SELECT operation_class, count FROM usage_log WHERE function_name = 'fastly.reconciliation' ORDER BY operation_class"
    ).fetchall()
    assert {r["operation_class"]: r["count"] for r in rows} == {"A": 5, "B": 3}

    for i in range(2):
        _seed_usage_log_row(sid, f"2026-05-22T14:0{i}:00Z", operation_class="A")
    record = [{"hour_iso": "2026-05-22T14:00:00Z", "class_a": 10, "class_b": 0}]
    assert metadata_db.reconcile_fastly_stats(sid, record) == 1
    assert metadata_db.reconcile_fastly_stats(sid, record) == 1
    r14 = con.execute(
        "SELECT count FROM usage_log WHERE function_name = 'fastly.reconciliation' AND operation_class = 'A' AND timestamp = ?",
        ("2026-05-22T14:00:00Z",),
    ).fetchall()
    assert len(r14) == 1 and r14[0]["count"] == 8

    for i in range(7):
        _seed_usage_log_row(sid, f"2026-05-22T15:0{i}:00Z", operation_class="A")
    assert (
        metadata_db.reconcile_fastly_stats(sid, [{"hour_iso": "2026-05-22T15:00:00Z", "class_a": 7, "class_b": 0}]) == 0
    )


# ── Postgres dialect shim & connection lifecycle ────────────────────────────


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_sql_rewriting_helpers():
    assert (
        pgc._replace_placeholders_outside_literals("SELECT * FROM t WHERE a = ? AND b = 'literal ? mark' AND c = ?")
        == "SELECT * FROM t WHERE a = %s AND b = 'literal ? mark' AND c = %s"
    )
    assert pgc._replace_placeholders_outside_literals("SELECT 1") == "SELECT 1"

    for table in ("sources", "rdns"):
        assert (
            pgc._rewrite_insert_or(f"INSERT OR IGNORE INTO {table} (a, b) VALUES (?, ?)")
            == f"INSERT INTO {table} (a, b) VALUES (?, ?) ON CONFLICT DO NOTHING"
        )

    qf_out = pgc._rewrite_insert_or("INSERT OR REPLACE INTO quarantined_files (service_id, file_name) VALUES (?, ?)")
    assert qf_out.startswith(
        "INSERT INTO quarantined_files (service_id, file_name) VALUES (?, ?) ON CONFLICT(service_id, file_name) DO UPDATE SET"
    )
    assert "source_name=EXCLUDED.source_name" in qf_out and "corrupt_rows=EXCLUDED.corrupt_rows" in qf_out

    v_out = pgc._rewrite_insert_or("INSERT OR REPLACE INTO views (id, service_id, name) VALUES (?, ?, ?)")
    assert "ON CONFLICT(id) DO UPDATE SET" in v_out and "service_id=EXCLUDED.service_id" in v_out

    with pytest.raises(NotImplementedError, match="brand_new_table"):
        pgc._rewrite_insert_or("INSERT OR IGNORE INTO brand_new_table (x) VALUES (?)")

    assert (
        pgc._rewrite_sql("INSERT INTO cron_runs (service_id, task) VALUES (?, ?)")
        == "INSERT INTO cron_runs (service_id, task) VALUES (%s, %s)"
    )
    assert "current_timestamp AT TIME ZONE 'UTC'" in pgc._rewrite_sql("SELECT datetime('now')")
    assert "current_timestamp AT TIME ZONE 'UTC' + INTERVAL '-15 minutes'" in pgc._rewrite_sql(
        "SELECT datetime('now', '-15 minutes')"
    )
    assert "to_char(current_timestamp AT TIME ZONE 'UTC'" in pgc._rewrite_sql(
        "SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"
    )
    instr_out = pgc._rewrite_sql("SELECT instr(a, b), substr(a, 1, 2) FROM t")
    assert "strpos(a, b)" in instr_out and "substring(a, 1, 2)" in instr_out

    assert "EXCLUDED.a" in pgc._rewrite_sql("INSERT INTO t VALUES (?) ON CONFLICT(id) DO UPDATE SET a = excluded.a")
    assert pgc._rewrite_sql("SELECT 'excluded.something'") == "SELECT 'excluded.something'"

    sql1, id1 = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s)")
    assert sql1.endswith("RETURNING id") and id1 == "id"
    sql2, id2 = pgc._maybe_add_returning("INSERT INTO ingested_files (a) VALUES (%s)")
    assert sql2 == "INSERT INTO ingested_files (a) VALUES (%s)" and id2 is None
    sql3, id3 = pgc._maybe_add_returning("INSERT INTO cron_runs (task) VALUES (%s) RETURNING id")
    assert sql3.count("RETURNING") == 1 and id3 is None


class _FakeCursor:
    def __init__(self, fetchone_result=None):
        self.executed: list[tuple[str, tuple]] = []
        self._fetchone_result = fetchone_result
        self.rowcount = 1

    def execute(self, sql, params=()):
        self.executed.append((sql, tuple(params)))

    def executemany(self, sql, seq):
        self.executed.append((sql, tuple(seq)))

    def fetchone(self):
        return self._fetchone_result

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self, fetchone_result=None):
        self._cursor = _FakeCursor(fetchone_result)

    def cursor(self):
        return self._cursor


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_connection_wrapper_execution_and_monitoring(monkeypatch):
    conn = _FakeConn()
    wrapper = pgc.PgConnectionWrapper(conn, pool=MagicMock())
    wrapper._service_id = "svc-1"
    seen: dict = {}
    orig_register = query_registry.query_registry.register

    def _spy_register(db_type, sql, **kwargs):
        seen["db_type"] = db_type
        seen["service_id"] = kwargs.get("service_id")
        return orig_register(db_type, sql, **kwargs)

    with patch.object(query_registry.query_registry, "register", side_effect=_spy_register):
        wrapper.execute("SELECT * FROM t WHERE a = ?", (1,))
    assert conn._cursor.executed[0] == ("SELECT * FROM t WHERE a = %s", (1,))
    assert seen == {"db_type": "Postgres", "service_id": "svc-1"}

    wrapper.commit()
    wrapper.rollback()

    c_cron = _FakeConn(fetchone_result={"id": 42})
    w_cron = pgc.PgConnectionWrapper(c_cron, pool=MagicMock())
    cur = w_cron.execute("INSERT INTO cron_runs (task) VALUES (?)", ("log_discovery",))
    assert cur.lastrowid == 42 and "RETURNING id" in c_cron._cursor.executed[0][0]
    assert wrapper.execute("INSERT INTO ingested_files (a) VALUES (?)", (1,)).lastrowid is None

    class _RaisingCursor(_FakeCursor):
        def execute(self, sql, params=()):
            raise RuntimeError("boom")

    c_err = _FakeConn()
    c_err._cursor = _RaisingCursor()
    w_err = pgc.PgConnectionWrapper(c_err, pool=MagicMock())
    with patch.object(query_registry.query_registry, "deregister") as mock_deregister:
        with pytest.raises(RuntimeError, match="boom"):
            w_err.execute("SELECT 1")
    assert mock_deregister.call_count == 1 and isinstance(mock_deregister.call_args[1].get("error"), RuntimeError)

    pool = MagicMock()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)
    w_noclose = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=False, pool=pool)
    w_noclose.close()
    pool.putconn.assert_not_called()
    w_close = pgc.PgConnectionWrapper(conn, return_to_pool_on_close=True, pool=pool)
    w_close.close()
    pool.putconn.assert_called_once_with(conn)


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_thread_and_readonly_connection_lifecycle(monkeypatch):
    pool = MagicMock()
    pool.getconn.side_effect = lambda: _FakeConn()
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    first = pgc.get_pg_thread_connection()
    assert first is pgc.get_pg_thread_connection() and pool.getconn.call_count == 1

    pgc.close_all_pg_connections()
    assert pool.putconn.call_count == 1
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 2

    pgc.release_pg_thread_connection()
    assert pool.putconn.call_count == 2
    pgc.release_pg_thread_connection()
    assert pool.putconn.call_count == 2

    a = pgc.get_pg_readonly_connection()
    b = pgc.get_pg_readonly_connection()
    assert a is not b
    a.close()
    b.close()


@pytest.mark.usefixtures("_reset_pg_state")
def test_pg_thread_isolation_and_weakset_gc(monkeypatch):
    pool = MagicMock()
    conns = [_FakeConn(), _FakeConn(), _FakeConn()]
    pool.getconn.side_effect = conns
    monkeypatch.setattr(pgc, "get_pg_pool", lambda: pool)

    pgc.get_pg_thread_connection()
    other_thread_wrapper = {}
    t = threading.Thread(target=lambda: other_thread_wrapper.setdefault("w", pgc.get_pg_thread_connection()))
    t.start()
    t.join()

    pgc.PgConnectionWrapper(_FakeConn())._finalizer()
    pgc.release_pg_thread_connection()
    owned_returns = [call.args[0] for call in pool.putconn.call_args_list if call.args[0] in conns]
    assert owned_returns == [conns[0]] and other_thread_wrapper["w"]._returned == [False]
    pgc.get_pg_thread_connection()
    assert pool.getconn.call_count == 3

    pgc.close_all_pg_connections()
    other_thread_wrapper.clear()
    pool.getconn.side_effect = lambda: _FakeConn()

    conn_gc = _FakeConn()
    w_gc = pgc.PgConnectionWrapper(conn_gc, pool=pool)
    del w_gc
    gc.collect()
    assert len([c for c in pool.putconn.call_args_list if c.args[0] is conn_gc]) == 1

    conn_closed = _FakeConn()
    w_closed = pgc.PgConnectionWrapper(conn_closed, return_to_pool_on_close=True, pool=pool)
    w_closed.close()
    del w_closed
    gc.collect()
    assert len([c for c in pool.putconn.call_args_list if c.args[0] is conn_closed]) == 1

    pgc.close_all_pg_connections()
    t2 = threading.Thread(target=pgc.get_pg_thread_connection)
    t2.start()
    t2.join()
    gc.collect()
    assert len(pgc._checked_out) == 0


@pytest.mark.usefixtures("_reset_pg_state")
def test_is_postgres_and_compat_row(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    assert pgc.is_postgres() is False
    monkeypatch.setenv("METADATA_DSN", "postgresql://x/y")
    assert pgc.is_postgres() is True

    row = pgc._CompatRow(["count", "name"], [7, "svc-a"])
    assert (row[0], row[1], row[0:2]) == (7, "svc-a", (7, "svc-a"))
    assert (row["count"], row["name"], row.get("count"), row.get("absent", "dflt")) == (7, "svc-a", 7, "dflt")
    assert dict(row) == {"count": 7, "name": "svc-a"} and list(row.keys()) == ["count", "name"]
    with pytest.raises(KeyError):
        _ = row["nope"]
    with pytest.raises(IndexError):
        _ = row[9]

    cur = MagicMock(description=None)
    assert pgc._compat_row_factory(cur)((1, 2)) == (1, 2)
    cur.description = [MagicMock(), MagicMock()]
    cur.description[0].name, cur.description[1].name = "a", "b"
    made = pgc._compat_row_factory(cur)((10, 20))
    assert made[0] == 10 and made["b"] == 20


# ── Metadata reconciliation (storage stats + cleanup) ──────────────────────


def _con(service_id: str):
    return metadata_db.get_con(service_id)


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = usage_log_db.get_con(service_id)
    now_ts = iso_z(datetime.now(UTC) - timedelta(days=days_ago))
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, bytes, count) "
        "VALUES (?, ?, 'A', 'PUT_OBJECT', 0, 1)",
        [(now_ts, service_id) for _ in range(rows)],
    )
    con.execute(
        """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
           VALUES (?, ?, 'A', 'PUT_OBJECT', ?, 0, ?)
           ON CONFLICT (service_id, hour, operation_class, operation_type)
           DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, last_updated = EXCLUDED.last_updated""",
        (service_id, now_ts[:13], rows, now_ts),
    )
    con.commit()


def _seed_ingested_file(service_id: str, rows: int, days_ago: int = 0, source_name: str = "fos") -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, ?, datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"raw/{source_name}-{days_ago}d-{i}.gz", source_name) for i in range(rows)],
    )
    con.commit()


def _seed_cron_run(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = _con(service_id)
    con.executemany(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        f"VALUES ('sync', datetime('now', '-{days_ago} days'), 1.0, 'success', '[]')",
        [() for _ in range(rows)],
    )
    con.commit()


@pytest.mark.usefixtures("_reset_pg_state")
def test_storage_stats_and_schema_tables():
    stats = reconciliation.get_metadata_storage_stats("svc-stats-fresh")
    for table in ("usage_log", "ingested_files", "cron_runs", "saved_views", "audit_log", "in_flight_buffers"):
        assert stats["tables"][table]["rows"] == 0
    assert stats["db_bytes"] is None and stats["db_path"] is None

    sid = "svc-stats-seeded"
    _seed_usage_log(sid, 7)
    _seed_ingested_file(sid, 3)
    _seed_cron_run(sid, 2)
    s2 = reconciliation.get_metadata_storage_stats(sid, force=True)
    assert (
        s2["tables"]["usage_log"]["rows"],
        s2["tables"]["ingested_files"]["rows"],
        s2["tables"]["cron_runs"]["rows"],
    ) == (7, 3, 2)
    assert s2["tables"]["cron_runs"]["bytes"] > 0 and s2["db_bytes"] is None

    existing = {
        r[0]
        for r in _con(sid).execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()
    }
    for sql_table, _ in reconciliation._STATS_TABLES:
        if sql_table != "usage_log":
            assert sql_table in existing


@pytest.mark.usefixtures("_reset_pg_state")
def test_is_ingested_files_dedup_active(monkeypatch):
    assert reconciliation.is_ingested_files_dedup_active("svc-no-cfg") is True
    for cfg, expected in (
        ({"provisioning": {"cron_sync": {"delete_after": True}}}, True),
        ({"provisioning": {"cron_sync": {"delete_after": False}}}, False),
        ({"provisioning": {"cron_sync": {"delete_after": True, "high_scale_shared_source": True}}}, False),
    ):
        monkeypatch.setattr(svcconfig, "load_config", lambda _s, c=cfg: c)
        assert reconciliation.is_ingested_files_dedup_active("svc-cfg") is expected


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_metadata_retention_and_events(monkeypatch):
    assert reconciliation._retention_timestamp_expr("ingested_at") == "CAST(ingested_at AS TIMESTAMPTZ)"

    sid = "svc-cleanup-aged"
    _seed_usage_log(sid, 5, days_ago=10)
    _seed_usage_log(sid, 3, days_ago=0)
    res = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 7})
    assert (res["deleted"]["usage_log"], res["after"]["usage_log"], res["vacuumed"]) == (5, 3, False)

    for bad_ret in (0, -5, "not-a-number"):
        sid_b = f"svc-cleanup-{bad_ret}"
        _seed_usage_log(sid_b, 4, days_ago=400)
        r_b = reconciliation.cleanup_metadata(sid_b, retention={"usage_log_days": bad_ret})
        assert (r_b["deleted"]["usage_log"], r_b["after"]["usage_log"]) == (0, 4)

    sid_def = "svc-cleanup-default"
    _seed_cron_run(sid_def, 4, days_ago=400)
    assert reconciliation.cleanup_metadata(sid_def)["deleted"]["cron_runs"] == 4

    sid_ev = "svc-cleanup-events"
    _seed_usage_log(sid_ev, 2, days_ago=10)
    events: list[dict] = []
    reconciliation.cleanup_metadata(
        sid_ev,
        retention={"usage_log_days": 1, "ingested_files_days": 1, "cron_runs_days": 1},
        on_event=events.append,
    )
    assert any(e["type"] == "status" for e in events) and any(e["type"] == "progress" for e in events)
    last_p = [e for e in events if e["type"] == "progress"][-1]
    assert last_p["current"] == last_p["total"]

    sid_bad_cb = "svc-cleanup-bad-cb"
    _seed_usage_log(sid_bad_cb, 3, days_ago=10)
    assert (
        reconciliation.cleanup_metadata(
            sid_bad_cb,
            retention={"usage_log_days": 1},
            on_event=lambda _e: (_ for _ in ()).throw(RuntimeError("cb fail")),
        )["deleted"]["usage_log"]
        == 3
    )

    assert (
        reconciliation.cleanup_metadata("svc-no-rollups", retention={"usage_log_days": 1, "rollups_days": 0})[
            "rollups_deleted"
        ]
        == 0
    )
    monkeypatch.setattr(_db, "get_source_for_service", lambda _s: None)
    assert (
        reconciliation.cleanup_metadata("svc-no-src", retention={"usage_log_days": 1, "rollups_days": 7})[
            "rollups_deleted"
        ]
        == 0
    )

    sid_fo = "svc-cleanup-forced-off"
    _seed_ingested_file(sid_fo, 5, days_ago=400)
    monkeypatch.setattr(svcconfig, "load_config", lambda _s: {"provisioning": {"cron_sync": {"delete_after": False}}})
    ev_fo: list[dict] = []
    r_fo = reconciliation.cleanup_metadata(sid_fo, retention={"ingested_files_days": 30}, on_event=ev_fo.append)
    assert (r_fo["deleted"]["ingested_files"], r_fo["after"]["ingested_files"]) == (0, 5)
    assert any(e["type"] == "status" and "dedup gate" in e.get("message", "") for e in ev_fo)


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_rum_ingested_files_and_no_reingest(monkeypatch):
    sid = "svc-cleanup-rum-dedup"
    _seed_ingested_file(sid, 3, days_ago=400)
    con = _con(sid)
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'fos', ?, datetime('now', '-400 days'), 1, 100)",
        [(f"raw/rum/old-{t}-{i}.gz", t) for t in ("client_vitals", "client_errors") for i in range(2)],
    )
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'fos', ?, datetime('now', '-10 days'), 1, 100)",
        [(f"raw/rum/recent-{t}.gz", t) for t in ("client_vitals", "client_errors")],
    )
    con.commit()
    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 1})
    assert (res["deleted"]["ingested_files"], res["after"]["ingested_files"]) == (7, 2)

    con.execute("DELETE FROM ingested_files")
    sid2 = "svc-cleanup-no-reingest"
    con2 = _con(sid2)
    old_file_key = "s3://test-bucket/raw/rum/year=2025/month=08/day=01/hour=12/minute=00/old.json.gz"
    con2.execute(
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        "VALUES (?, 'test_service', 'client_vitals', datetime('now', '-400 days'), 1, 100)",
        (old_file_key,),
    )
    con2.commit()
    reconciliation.cleanup_metadata(sid2, retention={"ingested_files_days": 1})
    assert con2.execute("SELECT COUNT(*) FROM ingested_files WHERE file_name = ?", (old_file_key,)).fetchone()[0] == 0

    mock_s3 = MagicMock()
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: mock_s3)
    monkeypatch.setattr(
        "backend.core.rum_ingest.get_source_for_service",
        lambda s: {"name": sid2, "bucket": "test-bucket", "service_id": sid2},
    )
    monkeypatch.setattr("backend.core.metadata.get_con", lambda s: con2)
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda s, t: 1)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", lambda *a, **kw: None)

    assert list(ingest_rum_logs(sid2)) == [("started", 1), ("done", 0)]
    assert con2.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0] == 0


def _poison_summary(service_id: str, *, file_count: int, total_rows: int, total_bytes: int) -> None:
    con = _con(service_id)
    con.execute(
        "INSERT INTO ingested_files_summary "
        "(source_name, file_count, total_rows, total_bytes, count_with_bytes, latest_file_name, last_ingested) "
        "VALUES (?, ?, ?, ?, ?, 'raw/poison.gz', datetime('now')) "
        "ON CONFLICT(source_name) DO UPDATE SET "
        "file_count=excluded.file_count, total_rows=excluded.total_rows, "
        "total_bytes=excluded.total_bytes, count_with_bytes=excluded.count_with_bytes",
        (service_id, file_count, total_rows, total_bytes, file_count),
    )
    con.commit()


@pytest.mark.usefixtures("_reset_pg_state")
def test_cleanup_ingested_files_summary_recompute():
    sid = "svc-rollup-recompute"
    _seed_ingested_file(sid, 5, days_ago=10, source_name=sid)
    _seed_ingested_file(sid, 3, days_ago=0, source_name=sid)
    _poison_summary(sid, file_count=999_999, total_rows=999_999, total_bytes=99_999_900)

    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})
    assert (res["deleted"]["ingested_files"], res["after"]["ingested_files"]) == (5, 3)
    summary = metadata_db.get_ingested_files_status_summary(sid)
    assert (summary["file_count"], summary["total_rows"], summary["total_bytes"]) == (3, 3, 300)

    sid2 = "svc-rollup-noop"
    _seed_ingested_file(sid2, 3, days_ago=0, source_name=sid2)
    _poison_summary(sid2, file_count=42, total_rows=4242, total_bytes=4200)
    assert reconciliation.cleanup_metadata(sid2, retention={"ingested_files_days": 7})["deleted"]["ingested_files"] == 0
    s2 = metadata_db.get_ingested_files_status_summary(sid2)
    assert (s2["file_count"], s2["total_rows"]) == (42, 4242)


# ── Cloud copy of committed Parquet (push) and restore ──────────────────────


def _cloud_env(monkeypatch, tmp_path):
    from backend.core.storage import backup
    from backend.core.storage._core import ducklake_table_name

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(tmp_path / "data"))
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    src = {"service_id": "svc_cloud", "name": "svc_cloud", "bucket": "test-bucket", "prefix": "pfx"}
    table = ducklake_table_name(src, "logs")
    con = duckdb.connect()
    assert ducklake._ducklake_attach(con, src, read_only=False)
    con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
    con.execute(
        f"CREATE TABLE lake.\"{table}\" AS SELECT i AS a, 'x'::VARCHAR AS b, 's3://test-bucket/raw/request/year=2026/f1.gz' AS _source_file FROM range(0, 100) t(i)"
    )
    con.execute(
        f"INSERT INTO lake.\"{table}\" SELECT i, 'y', 's3://test-bucket/raw/request/year=2026/f2.gz' FROM range(100, 200) t(i)"
    )
    return backup, src, table, con


def _keys(client, prefix="pfx/ducklake/", data_only=True):
    pages = client.get_paginator("list_objects_v2").paginate(Bucket="test-bucket", Prefix=prefix)
    keys = sorted(o["Key"] for p in pages for o in p.get("Contents", ()))
    return [k for k in keys if "/main/" in k] if data_only else keys


def test_push_to_cloud_uploads_live_files_and_removes_compacted_ones(monkeypatch, tmp_path, s3_mock):
    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        first = backup.push_to_cloud(src)
        assert first["uploaded"] == 2 and first["deleted"] == 0
        assert len(_keys(s3_mock)) == 2 and all(f"/main/{table}/" in k for k in _keys(s3_mock))
        quiet = backup.push_to_cloud(src)
        assert (quiet["uploaded"], quiet["deleted"], quiet["foreign"], quiet["adopted"]) == (0, 0, 0, True)

        con.execute(f"CALL ducklake_merge_adjacent_files('lake', '{table}')")
        con.execute("CALL ducklake_expire_snapshots('lake', older_than => now() + INTERVAL 1 DAY)")
        con.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
        merged = backup.push_to_cloud(src)
        assert merged["uploaded"] == 1 and merged["deleted"] == 2
        assert len(_keys(s3_mock)) == 1
        # The two superseded files keep billing for 30 days after deletion, so their bytes are recorded.
        from backend.core.metadata.usage_log import cloud_copy_deleted_bytes_in_billing_window

        assert merged["deleted_bytes"] > 0
        assert cloud_copy_deleted_bytes_in_billing_window("svc_cloud") == merged["deleted_bytes"]
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


def test_push_to_cloud_never_deletes_when_the_catalog_reports_no_live_files(monkeypatch, tmp_path, s3_mock):
    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        backup.push_to_cloud(src)
        before = _keys(s3_mock)
        con.execute(f'DROP TABLE lake."{table}"')
        con.execute(f'CREATE TABLE lake."{table}" (a BIGINT, b VARCHAR)')
        result = backup.push_to_cloud(src)
        assert result["deleted"] == 0 and _keys(s3_mock) == before
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


def test_push_to_cloud_skips_high_scale_and_missing_bucket(monkeypatch, tmp_path):
    from backend.core.storage import backup

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(tmp_path))
    assert "skipped" in backup.push_to_cloud({"service_id": "s", "name": "s"})
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", "s3://bkt/pfx/ducklake/")
    monkeypatch.setenv("ALLOW_REMOTE_S3_PARQUET_SCAN", "1")
    assert "skipped" in backup.push_to_cloud({"service_id": "s", "name": "s", "bucket": "bkt"})


@pytest.mark.parametrize("lose_catalog", [False, True])
def test_restore_from_cloud_rebuilds_lost_local_data(monkeypatch, tmp_path, s3_mock, lose_catalog):
    import shutil

    from backend.core.duckdb import _purge_dropped_catalog_rows

    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        backup.push_to_cloud(src)
        con.execute(f'DROP TABLE lake."{table}"') if lose_catalog else None
        con.close()
        if lose_catalog:
            _purge_dropped_catalog_rows("svc_cloud", [table])
        shutil.rmtree(tmp_path / "data")

        result = backup.restore_from_cloud(src)
        assert result["downloaded"] == 2 and result["registered"] == (2 if lose_catalog else 0)
        assert result["reindexed"] == 2
        from backend.core.metadata.ingest_log import get_ingested_filenames

        assert get_ingested_filenames("svc_cloud") == {
            "s3://test-bucket/raw/request/year=2026/f1.gz",
            "s3://test-bucket/raw/request/year=2026/f2.gz",
        }

        con = duckdb.connect()
        assert ducklake._ducklake_attach(con, src, read_only=False)
        assert con.execute(f'SELECT count(*), min(a), max(a) FROM lake."{table}"').fetchone() == (200, 0, 199)
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


def test_schedule_cloud_push_is_single_flight_and_coalesces_requests(monkeypatch):
    import threading

    from backend.core.storage import backup

    started, release, calls = threading.Event(), threading.Event(), []

    def fake_push(source):
        calls.append(source)
        started.set()
        release.wait(5)
        return {}

    monkeypatch.setattr(backup, "push_to_cloud", fake_push)
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr(backup, "is_high_scale_mode", lambda _src: False)
    src = {"service_id": "svc_coalesce", "name": "svc_coalesce", "bucket": "b"}
    backup.schedule_cloud_push(src)
    assert started.wait(5)
    backup.schedule_cloud_push(src)
    backup.schedule_cloud_push(src)
    release.set()
    deadline = time.time() + 5
    while time.time() < deadline and "svc_coalesce" in backup._running:
        time.sleep(0.02)
    assert len(calls) == 2


def test_schedule_cloud_push_does_nothing_without_a_bucket_or_in_dev_no_crons(monkeypatch):
    from backend.core.storage import backup

    ran = []
    monkeypatch.setattr(backup, "push_to_cloud", lambda *a: ran.append(a))
    monkeypatch.setattr(backup, "is_high_scale_mode", lambda _src: False)
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    backup.schedule_cloud_push({"service_id": "s", "name": "s"})
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    backup.schedule_cloud_push({"service_id": "s", "name": "s", "bucket": "b"})
    assert ran == [] and not backup._running


# ── Cloud copy adoption: never delete a backup the current catalog did not write ──


def _put_remote(client, key, body=b"old-parquet"):
    client.put_object(Bucket="test-bucket", Key=key, Body=body)


def test_push_never_deletes_a_copy_that_predates_the_catalog(monkeypatch, tmp_path, s3_mock):
    from datetime import UTC, datetime, timedelta

    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        old = [f"pfx/ducklake/main/{table}/old-{i}.parquet" for i in range(3)]
        for key in old:
            _put_remote(s3_mock, key)
        uuid = backup._catalog_identity(src)[0]
        # A catalog created after those objects were written: the whole old backup looks foreign to it.
        monkeypatch.setattr(backup, "_catalog_identity", lambda _s: (uuid, datetime.now(UTC) + timedelta(hours=1)))
        result = backup.push_to_cloud(src)
        assert result["deleted"] == 0 and result["foreign"] == 3 and result["adopted"] is False
        assert set(old) <= set(_keys(s3_mock)), "the old backup must survive"
        assert "pfx/ducklake/_catalog.json" not in _keys(s3_mock, data_only=False), "a foreign copy is not adopted"
        # More commits and compaction in the fresh catalog still must not touch the old files.
        con.execute(f"CALL ducklake_merge_adjacent_files('lake', '{table}')")
        con.execute("CALL ducklake_expire_snapshots('lake', older_than => now() + INTERVAL 1 DAY)")
        con.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
        assert backup.push_to_cloud(src)["deleted"] == 0 and set(old) <= set(_keys(s3_mock))
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


def test_push_adopts_a_copy_that_only_holds_this_catalogs_files_and_then_mirrors(monkeypatch, tmp_path, s3_mock):
    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        stray = f"pfx/ducklake/main/{table}/stray.parquet"
        _put_remote(s3_mock, stray)  # newer than the catalog's oldest snapshot, not live: this catalog's garbage
        result = backup.push_to_cloud(src)
        assert result["deleted"] == 1 and result["foreign"] == 0 and result["adopted"] is True
        assert stray not in _keys(s3_mock) and "pfx/ducklake/_catalog.json" in _keys(s3_mock, data_only=False)
        # Adopted: later garbage is removed with no age condition.
        late = f"pfx/ducklake/main/{table}/late.parquet"
        _put_remote(s3_mock, late)
        from datetime import UTC, datetime, timedelta

        uuid = backup._catalog_identity(src)[0]
        monkeypatch.setattr(backup, "_catalog_identity", lambda _s: (uuid, datetime.now(UTC) + timedelta(hours=1)))
        assert backup.push_to_cloud(src)["deleted"] == 1 and late not in _keys(s3_mock)
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


def test_a_marker_for_another_catalog_does_not_count_as_adoption(monkeypatch, tmp_path, s3_mock):
    import json
    from datetime import UTC, datetime, timedelta

    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        s3_mock.put_object(
            Bucket="test-bucket", Key="pfx/ducklake/_catalog.json", Body=json.dumps({"catalog_uuid": "someone-else"})
        )
        old = f"pfx/ducklake/main/{table}/old.parquet"
        _put_remote(s3_mock, old)
        uuid = backup._catalog_identity(src)[0]
        monkeypatch.setattr(backup, "_catalog_identity", lambda _s: (uuid, datetime.now(UTC) + timedelta(hours=1)))
        result = backup.push_to_cloud(src)
        assert result["adopted"] is False and result["deleted"] == 0 and old in _keys(s3_mock)
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


def test_restore_adopts_the_copy_so_later_pushes_can_mirror_it(monkeypatch, tmp_path, s3_mock):
    import json
    import shutil
    from datetime import UTC, datetime, timedelta

    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        backup.push_to_cloud(src)
        con.close()
        shutil.rmtree(tmp_path / "data")
        backup.restore_from_cloud(src)
        marker = json.loads(s3_mock.get_object(Bucket="test-bucket", Key="pfx/ducklake/_catalog.json")["Body"].read())
        uuid = backup._catalog_identity(src)[0]
        assert marker["catalog_uuid"] == uuid
        _put_remote(s3_mock, f"pfx/ducklake/main/{table}/superseded.parquet")
        monkeypatch.setattr(backup, "_catalog_identity", lambda _s: (uuid, datetime.now(UTC) + timedelta(hours=1)))
        result = backup.push_to_cloud(src)
        assert result["adopted"] is True and result["deleted"] == 1
        con = duckdb.connect()
        assert ducklake._ducklake_attach(con, src, read_only=False)
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


@pytest.fixture
def _fresh_catalog_dsn(_pg_worker_schema):
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(ducklake.config.DUCKLAKE_CATALOG)
    worker_db, admin = _pg_worker_schema
    fresh_name = f"{worker_db}_fresh_catalog"
    fresh_dsn = urlunsplit((parts.scheme, parts.netloc, "/" + fresh_name, parts.query, ""))
    if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (fresh_name,)).fetchone():
        admin.execute(f'CREATE DATABASE "{fresh_name}"')
    with psycopg.connect(fresh_dsn, autocommit=True) as catalog:
        catalog.execute("DROP SCHEMA IF EXISTS public CASCADE")
        catalog.execute("CREATE SCHEMA public")
        try:
            yield fresh_dsn
        finally:
            # DROP DATABASE waits on a checkpoint; the worker's exclusive slot permits reuse.
            catalog.execute("DROP SCHEMA public CASCADE")
            catalog.execute("CREATE SCHEMA public")


def test_expiry_bills_the_bytes_of_high_scale_files_when_the_cleanup_unlinks_them(
    monkeypatch, tmp_path, _fresh_catalog_dsn
):
    from backend.core.metadata.usage_log import clear_usage_log, cloud_copy_deleted_bytes_in_billing_window
    from backend.core.storage import _core
    from backend.core.storage._core import ducklake_table_name

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(tmp_path / "data"))
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", _fresh_catalog_dsn)
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    src = {"service_id": "svc_hs_billing", "name": "svc_hs_billing", "bucket": "test-bucket"}
    clear_usage_log("svc_hs_billing")
    table = ducklake_table_name(src, "logs")
    con = duckdb.connect()
    try:
        assert ducklake._ducklake_attach(con, src, read_only=False)
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.execute(f'CREATE TABLE lake."{table}" AS SELECT i AS a FROM range(0, 100) t(i)')
        con.execute(f'INSERT INTO lake."{table}" SELECT i FROM range(100, 200) t(i)')
        before = {os.path.basename(str(p)): p.stat().st_size for p in (tmp_path / "data").rglob("*.parquet")}
        con.execute(f"CALL ducklake_merge_adjacent_files('lake', '{table}')")
        monkeypatch.setattr(_core, "_remote_data_path", lambda _s: True)
        monkeypatch.setattr(
            _core, "_resolve_data_path", lambda _s, _sid: "s3://test-bucket/" + str(tmp_path / "data").lstrip("/") + "/"
        )
        monkeypatch.setattr(_core, "_fos_object_size", lambda _s, _b, key: before.get(os.path.basename(key), 0))
        time.sleep(0.2)
        out = _core._ducklake_expire_snapshots(con, src, keep_snapshot_days=0)
        assert out["data_files_cleaned"] == 2
        assert out["fos_deleted_bytes_billed"] == sum(before.values())
        assert cloud_copy_deleted_bytes_in_billing_window("svc_hs_billing") == sum(before.values())
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()
        clear_usage_log("svc_hs_billing")


def test_restore_brings_back_saved_views_from_the_admin_state_snapshot(monkeypatch, tmp_path, s3_mock):
    import json
    import shutil

    from backend.core import metadata as metadata_db

    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    try:
        backup.push_to_cloud(src)
        con.close()
        shutil.rmtree(tmp_path / "data")
        view = {
            "id": "view-1",
            "name": "Errors today",
            "filters_json": "{}",
            "time_range_type": "relative",
            "page": "dashboard",
        }
        s3_mock.put_object(
            Bucket="test-bucket", Key="pfx/meta/admin_state.json", Body=json.dumps({"_views": [view, {"id": "x"}]})
        )
        metadata_db.delete_view("svc_cloud", "view-1")
        assert backup.restore_from_cloud(src)["views_restored"] == 1
        assert [v["name"] for v in metadata_db.list_views("svc_cloud")] == ["Errors today"]
        backup.restore_from_cloud(src)
        assert len(metadata_db.list_views("svc_cloud")) == 1
        metadata_db.delete_view("svc_cloud", "view-1")
        con = duckdb.connect()
        assert ducklake._ducklake_attach(con, src, read_only=False)
    finally:
        con.execute(f'DROP TABLE IF EXISTS lake."{table}"')
        con.close()


def test_a_fresh_real_catalog_never_deletes_the_old_backup(monkeypatch, tmp_path, s3_mock, _fresh_catalog_dsn):
    """A second real catalog must preserve the first catalog's cloud copy."""
    old_dsn = ducklake.config.DUCKLAKE_CATALOG
    backup, src, table, con = _cloud_env(monkeypatch, tmp_path)
    con_b = None
    try:
        backup.push_to_cloud(src)
        old_identity = backup._catalog_identity(src)
        old_keys = _keys(s3_mock)
        assert len(old_keys) == 2 and "pfx/ducklake/_catalog.json" in _keys(s3_mock, data_only=False)
        con.execute(f'DROP TABLE lake."{table}"')
        con.close()
        time.sleep(1.2)
        monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", _fresh_catalog_dsn)
        monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(tmp_path / "data_fresh"))
        con_b = duckdb.connect()
        assert ducklake._ducklake_attach(con_b, src, read_only=False)
        con_b.execute(f"CREATE TABLE lake.\"{table}\" AS SELECT i AS a, 'z'::VARCHAR AS b FROM range(0, 10) t(i)")
        new_identity = backup._catalog_identity(src)
        assert old_identity is not None and new_identity is not None
        assert new_identity[0] != old_identity[0]

        result = backup.push_to_cloud(src)
        assert result["adopted"] is False and result["deleted"] == 0 and result["foreign"] == 2
        assert set(old_keys) <= set(_keys(s3_mock)), "the old copy must survive a fresh catalog"
        assert result["uploaded"] == 1, "the new data still uploads beside the old copy"
        con_b.close()
        con_b = None
        restored = backup.restore_from_cloud(src)
        assert restored["registered"] == 2 and backup.push_to_cloud(src)["adopted"] is True
    finally:
        if con_b is not None:
            con_b.close()
        con.close()
        monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", old_dsn)
