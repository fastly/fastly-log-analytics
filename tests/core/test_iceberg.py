"""Tests for DuckLake / Iceberg buffer, view, schema, cloud-write locks, catalog helpers, and local compaction."""

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
from backend.core._log_fields_data import LOG_FIELD_CATALOG
from backend.core.iceberg._ducklake import (
    DuckLakeAdmissionTimeout,
    _ducklake_attach,
    _reset_for_tests,
    ducklake_table_name,
    ducklake_write_admission,
    get_admission_stats,
)
from backend.core.iceberg.manifest import ducklake_table_exists, get_snapshot_calendar, get_table_info
from backend.core.iceberg.view import _get_service_lock, _source_variant_fp
from backend.core.rollups.time_series import build_time_series_bundles
from tests._helpers.sources import make_source

from backend import config as svcconfig
from backend.core import duckdb as db
from backend.core import iceberg
from backend.core import local_compaction as lc
from backend.core import reset as reset_mod
from backend.core.duckdb import get_connection
from backend.core.duckdb_pool import _Pool
from backend.core.iceberg import _core
from backend.core.iceberg import _ducklake as ducklake
from backend.core.iceberg import buffer as buffer_mod
from backend.core.iceberg.buffer import _commit_buffer_impl, _ducklake_write_connection, buffer_files
from backend.core.ingest import get_catalog_field_ids
from backend.core.local_compaction import _compact_single_partition

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
    aligned = iceberg._align_to_schema(table)
    arrow_schema = iceberg.get_arrow_schema()
    assert len(aligned.schema) == len(arrow_schema)
    assert "timestamp" in aligned.column_names and "ip" in aligned.column_names
    assert "extra_field" not in aligned.column_names
    assert "status" in aligned.column_names and aligned.column("status").null_count == len(aligned)


@pytest.mark.parametrize("cfg", [None, {}, {"custom_fields": []}])
def test_get_arrow_schema_no_custom_fields(cfg):
    base_schema = iceberg.get_arrow_schema()
    schema = iceberg.get_arrow_schema(cfg)
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
    schema = iceberg.get_arrow_schema(config)
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
                "iceberg_type": "string",
                "vcl_log_expression": "X",
                "enabled": True,
            },
            {
                "name": "aaa_first",
                "duckdb_type": "VARCHAR",
                "iceberg_type": "string",
                "vcl_log_expression": "X",
                "enabled": True,
            },
            {
                "name": "inactive_field",
                "duckdb_type": "VARCHAR",
                "iceberg_type": "string",
                "vcl_log_expression": "X",
                "enabled": False,
            },
        ],
    }
    schema = iceberg.get_arrow_schema(config)
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
    schema_names = {f.name for f in iceberg.get_arrow_schema()}
    assert not (storage_ids - schema_names)


# --- Catalog Initialization & View Contention Tests ---


def test_init_iceberg_table_attaches_catalog_without_creating_table(tmp_path):
    source = _ducklake_source(tmp_path, "init")
    assert iceberg.init_iceberg_table(source) is True
    assert iceberg.init_iceberg_table(source, create=True) is True
    assert ducklake_table_exists(source) is False


def test_init_iceberg_table_returns_none_when_catalog_unattachable(tmp_path):
    source = _ducklake_source(tmp_path, "initfail")
    fake_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=False),
    ):
        assert iceberg.init_iceberg_table(source) is None
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
def test_update_iceberg_view_lock_contention(
    monkeypatch, source_key, persisted_row, hold_s, lock_timeout, expected_calls
):
    iceberg._view_cache.pop(source_key, None)
    iceberg._service_locks.pop(source_key, None)
    source = {"name": source_key}
    locked_calls: list = []
    monkeypatch.setattr(iceberg, "_update_iceberg_view_locked", lambda con, src: locked_calls.append((con, src)))

    lock = iceberg._get_service_lock(source_key)
    fake_con = MagicMock()
    fake_con.execute.return_value.fetchone.return_value = persisted_row
    try:
        with _hold_lock_in_thread(lock, wait_s=hold_s):
            iceberg.update_iceberg_view(fake_con, source, lock_timeout=lock_timeout)
        assert len(locked_calls) == expected_calls
    finally:
        iceberg._view_cache.pop(source_key, None)
        iceberg._service_locks.pop(source_key, None)


def test_source_variant_fp_changes_when_access_or_time_range_changes():
    base = {"name": "svc-002", "access_level": "read_write"}
    assert _source_variant_fp(base) == ("read_write", (), True)
    analyst = {"name": "svc-002", "access_level": "read_only"}
    assert _source_variant_fp(analyst) == ("read_only", (), True)
    assert _source_variant_fp(analyst) != _source_variant_fp(base)

    bounded = {
        "name": "svc-002",
        "access_level": "read_only",
        "time_range": {"start": "2026-06-01T00:00:00Z", "end": "2026-06-02T00:00:00Z"},
    }
    fp_bounded = _source_variant_fp(bounded)
    assert fp_bounded != _source_variant_fp(analyst)
    assert fp_bounded[1] == ("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z")

    cron_disabled = {"name": "svc-002", "access_level": "read_write", "provisioning": {"cron_sync": {"enabled": False}}}
    assert _source_variant_fp(cron_disabled) != _source_variant_fp(base)


# --- Compaction & Cloud Write Lock Serialization Tests ---


def test_optimize_table_hostile_service_id_cannot_break_out_of_call_literal(monkeypatch):
    executed = _recording_write_connection(monkeypatch)
    source = {"name": "svc", "service_id": "O'Brien'); DROP TABLE lake.logs;--"}
    iceberg.optimize_table(source, table_name="logs")

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

    result = iceberg.optimize_table(source)
    assert not result.get("error") and result["partition_errors"] == []

    con = get_connection(source)
    try:
        rows = con.execute(f'SELECT ip, edge_sid FROM lake."{ducklake_table_name(source)}" ORDER BY ip').fetchall()
    finally:
        con.close()
    assert rows == [("10.0.0.0", None), ("10.0.0.1", None), ("10.0.0.10", "evolved"), ("10.0.0.11", "evolved")]


def test_optimize_table_merge_failure_is_recorded_not_retried_and_rewrite_still_runs(monkeypatch):
    executed = _recording_write_connection(monkeypatch, fail_on="ducklake_merge_adjacent_files")
    result = iceberg.optimize_table({"name": "svc", "service_id": "svc-opt"}, table_name="logs")

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
    monkeypatch.setattr("backend.core.iceberg._core.init_iceberg_table", MagicMock())
    monkeypatch.setattr("backend.core.iceberg.view.update_iceberg_view", MagicMock())
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
            "access_level": "read_write",
            "provisioning": {"cron_sync": {"enabled": True}},
        },
    )
    reached_middle, release_reset = threading.Event(), threading.Event()

    def _slow_delete(*args, **kwargs):
        reached_middle.set()
        release_reset.wait(timeout=5)
        return 0

    monkeypatch.setattr("backend.core.ingest._delete_objects_robust", _slow_delete)
    s3_mock.put_object(Bucket=fos_source["bucket"], Key="iceberg/default/logs/data/foo.parquet", Body=b"x")
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
        assert iceberg._buffer_dir({"name": "test-svc"}) == "cache/test-svc/buffer"
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        assert iceberg._buffer_dir({"name": "svc"}) == str(tmp_path / "buffer")


def test_tombstone_buffer_files_writes_sidecar_marker_and_leaves_parquet(tmp_path):
    src, paths = _make_buffer(tmp_path, "batch_A.parquet", "batch_B.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        tombstoned = iceberg.tombstone_buffer_files(src, paths, ts=1717_000_000)
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
        tombstoned = iceberg.tombstone_buffer_files(src, paths, ts=ts)
    assert os.path.exists(paths[0]) and paths[0] not in tombstoned


def test_buffer_files_excludes_tombstoned_parquets_and_markers(tmp_path):
    src, paths = _make_buffer(tmp_path, "batch_keep.parquet", "batch_consumed.parquet")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg.tombstone_buffer_files(src, [paths[1]], ts=1717_000_000)
        assert os.path.exists(paths[1] + ".consumed-1717000000")
        listing = iceberg.buffer_files(src)
    assert listing == [paths[0]] and all(not entry.endswith("1717000000") for entry in listing)


@pytest.mark.parametrize(
    ("batch_name", "now", "expected_swept", "should_exist"),
    [("batch_young.parquet", 2_000_000_010, 0, True), ("batch_old.parquet", 2_000_000_090, 1, False)],
)
def test_sweep_tombstoned_buffer_files_grace_window(tmp_path, batch_name, now, expected_swept, should_exist):
    src, paths = _make_buffer(tmp_path, batch_name)
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        iceberg.tombstone_buffer_files(src, paths, ts=2_000_000_000)
        swept = iceberg.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=now)
    assert swept == expected_swept
    assert os.path.exists(paths[0]) is should_exist
    assert os.path.exists(paths[0] + ".consumed-2000000000") is should_exist


def test_sweep_tolerates_malformed_marker_filenames(tmp_path):
    src, _ = _make_buffer(tmp_path, "batch_X.parquet")
    buf = tmp_path / "buffer"
    (buf / "batch_X.parquet.consumed-notanumber").write_bytes(b"")
    (buf / "batch_X.parquet.consumed-").write_bytes(b"")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        assert iceberg.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_000) == 0


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
            iceberg.tombstone_buffer_files(src, [parquet_path], ts=2_000_000_000)
            assert con.execute("SELECT count(*) FROM race_view").fetchone() == (3,)
            assert iceberg.buffer_files(src) == []
            swept = iceberg.sweep_tombstoned_buffer_files(src, grace_seconds=60, now=2_000_000_090)
            assert swept == 1 and not os.path.exists(parquet_path)
    finally:
        con.close()


# --- View Caches, Locks & Debug Overlay Helpers ---


def test_clear_and_invalidate_service_caches():
    iceberg._view_cache["test-svc"] = ("loc", set(), (), "sql", 1.0, True)
    iceberg.clear_source_caches("test-svc")
    assert "test-svc" not in iceberg._view_cache
    iceberg.clear_source_caches("never-seen-svc")

    src = {"name": "inv-test-svc", "bucket": "inv-test-bucket", "prefix": ""}
    _core._view_cache[src["name"]] = ("loc", set(), (), "sql", 1.0, True)
    iceberg.invalidate_service_caches(src)
    assert src["name"] not in _core._view_cache
    iceberg.invalidate_service_caches({"name": "never-seen", "bucket": "nb", "prefix": ""})


def test_get_service_lock_identity_and_isolation():
    assert _get_service_lock("svc-X") is _get_service_lock("svc-X")
    assert _get_service_lock("svc-A") is not _get_service_lock("svc-B")


def test_get_last_view_stats():
    iceberg._view_cache.pop("never-seen", None)
    assert iceberg.get_last_view_stats({"name": "never-seen"}) == {}
    iceberg._view_cache["test-svc-stats"] = (
        "metadata_loc",
        frozenset(),
        ("col1", "col2"),
        "CREATE VIEW x AS ...",
        12.3,
        True,
    )
    assert iceberg.get_last_view_stats({"name": "test-svc-stats"}) == {
        "sql": "CREATE VIEW x AS ...",
        "time_ms": 12.3,
        "was_fast_path": True,
    }


@pytest.mark.parametrize(
    ("svc_name", "was_fast_path", "time_ms", "expected_label"),
    [("test-svc-inject", True, 5.0, "FAST PATH"), ("test-svc-slow", False, 50.0, "SLOW PATH")],
)
def test_inject_view_debug_prepends_formatted_row(svc_name, was_fast_path, time_ms, expected_label):
    iceberg._view_cache[svc_name] = ("loc", frozenset(), (), "SELECT 1", time_ms, was_fast_path)
    debug_list = [{"sql": "user query", "time_ms": 100}]
    iceberg.inject_view_debug(debug_list, {"name": svc_name})
    assert "View Resolution" in debug_list[0]["sql"] and expected_label in debug_list[0]["sql"]
    assert debug_list[1]["sql"] == "user query"


def test_inject_view_debug_is_noop_when_no_cached_stats():
    iceberg._view_cache.pop("never-seen-inject", None)
    debug_list = [{"sql": "user query", "time_ms": 100}]
    iceberg.inject_view_debug(debug_list, {"name": "never-seen-inject"})
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
        patch("backend.core.iceberg._align_to_schema", return_value=fake_table),
        patch("backend.core.iceberg.buffer.pq.write_table") as mock_write,
        patch("os.rename"),
    ):
        out = iceberg.write_to_buffer({"name": "svc"}, fake_table, "x.parquet")
    assert (target / "buffer").exists() and out == str(target / "buffer" / "x.parquet")
    mock_write.assert_called_once()
    _, kwargs = mock_write.call_args
    assert kwargs.get("compression") == "zstd" and kwargs.get("compression_level") == 1


def test_commit_buffer_returns_zero_summary_when_no_buffer_files(tmp_path):
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.iceberg._init_iceberg_table_locked") as mock_init,
    ):
        out = iceberg.commit_buffer({"name": "svc"})
    assert out == {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}
    mock_init.assert_not_called()


def test_quarantine_buffer_file_moves_with_sidecar_and_handles_collision(tmp_path):
    cache_root = tmp_path / "cache-root"
    buffer_dir = cache_root / "bkt" / "buffer"
    buffer_dir.mkdir(parents=True)
    bad = buffer_dir / "batch_deadbeef.parquet"
    bad.write_bytes(b"not a parquet file")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        new_path = iceberg._quarantine_buffer_file({"name": "svc"}, str(bad), RuntimeError("parquet footer is invalid"))

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
        p1 = iceberg._quarantine_buffer_file({"name": "svc"}, str(bad1), ValueError("first"))
    bad2 = buffer_dir / "batch_x.parquet"
    bad2.write_bytes(b"second bad")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root / "bkt")):
        p2 = iceberg._quarantine_buffer_file({"name": "svc"}, str(bad2), ValueError("second"))
    assert p1 != p2 and os.path.exists(p1) and os.path.exists(p2)


def test_commit_buffer_quarantines_unreadable_files_and_ignores_on_next_run(tmp_path):
    source = _ducklake_source(tmp_path, "qh")
    good = _write_buffer_parquet(source, "batch_good.parquet", rows=3)
    buffer_dir = good.parent
    bad = buffer_dir / "batch_bad.parquet"
    bad.write_bytes(b"definitely not parquet")

    result = iceberg.commit_buffer(source)
    assert result["files_committed"] == 1 and result["quarantined_files"] == 1 and result["rows_committed"] == 3
    assert _lake_row_count(source) == 3
    assert good.exists() and len(list(good.parent.glob(good.name + ".consumed-*"))) == 1
    assert not bad.exists()
    quarantine_dir = buffer_dir / ".quarantine"
    assert len(list(quarantine_dir.glob("*batch_bad.parquet"))) == 1
    assert len(list(quarantine_dir.glob("*.parquet.json"))) == 1

    _write_buffer_parquet(source, "batch_good_2.parquet", rows=1, offset=10)
    second = iceberg.commit_buffer(source)
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

    result = iceberg.commit_buffer(source)
    assert attaches["n"] == 3 and result["files_committed"] == 7 and result["rows_committed"] == 7
    assert _lake_row_count(source) == 7
    for p in paths:
        assert list(p.parent.glob(p.name + ".consumed-*"))
    assert iceberg.buffer_files(source) == []


def test_buffer_backlog_stats_empty_and_populated(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "empty")):
        assert iceberg.buffer_backlog_stats({"name": "svc"}) == {
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
        stats = iceberg.buffer_backlog_stats({"name": "svc"})
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

    with patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=Cursor()):
        with ducklake_write_admission("admission-postgres", timeout_s=0.2):
            pass

    sqls = [c[0] for c in calls]
    assert not any("pg_try_advisory_lock" in s for s in sqls)
    assert any(s.startswith("SET lock_timeout") for s in sqls)
    acq_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_lock(" in s)
    rel_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_unlock(" in s)
    assert acq_idx < rel_idx and calls[acq_idx][1][0] == calls[rel_idx][1][0]

    calls.clear()
    with patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=Cursor(fail_lock=True)):
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


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_serving_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")
    temp_dir = tmp_path / "duckdb-temp"
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(temp_dir))
    source = _durable_source(tmp_path)

    with (
        patch("backend.core.duckdb._configure_fos"),
        patch("backend.core.iceberg.configure_duckdb_s3"),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=True),
        patch("backend.core.iceberg.update_iceberg_view"),
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
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 365)
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 365)
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
    monkeypatch.setattr("backend.core.local_compaction._MAX_PARTITION_BYTES", 100)
    result = lc.compact_local_partitions(patched_cache_dir)
    assert result["partitions_compacted"] == 0 and len([f for f in os.listdir(part) if f.endswith(".parquet")]) == 5


def test_daily_tier_rolls_up_old_partitions(patched_cache_dir, monkeypatch):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 0)
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
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 0)
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
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 0)
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
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 0)
    day = "2026-05-15"
    paths = []
    for hh in ("00", "01", "02"):
        part = os.path.join(data_dir, f"timestamp_hour={day}-{hh}")
        os.makedirs(part)
        p = os.path.join(part, "f0.parquet")
        _write_parquet(p, rows=10)
        paths.append(p)

    monkeypatch.setattr("backend.core.local_compaction._MAX_PARTITION_BYTES", os.path.getsize(paths[0]) + 50)
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
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 0)
    paths = []
    for day in ("2026-05-04", "2026-05-05", "2026-05-06"):
        p = os.path.join(daily_dir, f"daily_{day}_abc12345.parquet")
        _write_parquet(p, rows=10)
        paths.append(p)

    monkeypatch.setattr("backend.core.local_compaction._MAX_PARTITION_BYTES", os.path.getsize(paths[0]) + 50)
    lc.compact_local_partitions(patched_cache_dir)
    weekly_dir = os.path.join(data_dir, "weekly")
    weekly_files = sorted([f for f in os.listdir(weekly_dir) if f.endswith(".parquet")])
    assert len(weekly_files) == 3 and all(f.startswith("weekly_2026-W19_") for f in weekly_files)
    with closing(duckdb.connect(":memory:")) as con:
        assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{weekly_dir}/*.parquet')").fetchone()[0] == 30
    assert not [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]


def test_daily_tier_migrates_single_file_bins_and_removes_dir(patched_cache_dir, monkeypatch):
    data_dir = os.path.join(patched_cache_dir["_test_cache_root"], "data")
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 0)
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
