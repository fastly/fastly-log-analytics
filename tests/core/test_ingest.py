from __future__ import annotations

import inspect
import os
import string
import threading
import time
from datetime import UTC, datetime
from functools import partial
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, consumes, initialize, invariant, rule
from tests._helpers.ledger import clear_ledger
from tests._helpers.sources import make_source

from backend.core import duckdb as _ddb
from backend.core import ingest
from backend.core import metadata as metadata_db
from backend.core.ingest import (
    _compute_incremental_start_after,
    _delete_objects_robust,
    _delete_objects_robust_with_failures,
    _parse_fastly_filename_dt,
    get_catalog_field_ids,
    get_ingest_columns_sql,
    get_ingest_type_hints,
)
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.metadata import base as metadata_base
from backend.core.metadata.base import get_con
from tests.core.test_ingest_discovery import (  # reuse fixture helpers
    _install_mock_fos,
    _S3RewritingConn,
    _seed_local,
    ingest_local_env,  # noqa: F401 — pytest fixture re-export
)


def test_ingest_type_hints():
    hints = get_ingest_type_hints()
    assert len(hints) == len([f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None])
    assert hints["timestamp"] == "TIMESTAMPTZ"
    ip_field = next(f for f in LOG_FIELD_CATALOG if f["id"] == "ip")
    assert hints["ip"] == ip_field["duckdb_type"]


def test_ingest_type_hints_with_custom_fields():
    config = {
        "custom_fields": [
            {"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "my_int", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "my_disabled", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    hints = get_ingest_type_hints(config)
    assert hints["my_field"] == "VARCHAR" and hints["my_int"] == "BIGINT"
    assert "my_disabled" not in hints and hints["timestamp"] == "TIMESTAMPTZ"


def test_get_catalog_field_ids_with_custom_fields():
    config = {
        "custom_fields": [
            {"name": "zzz_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "aaa_field", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "disabled_field", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    ids = get_catalog_field_ids(config)
    assert ids.index("aaa_field") < ids.index("zzz_field")
    assert "disabled_field" not in ids and "timestamp" in ids


def test_get_ingest_columns_sql_renders_duckdb_types_map():
    sql = get_ingest_columns_sql()
    assert sql.startswith("{") and sql.endswith("}") and "'timestamp': 'TIMESTAMPTZ'" in sql


def test_get_ingest_columns_sql_includes_enabled_custom_fields_only():
    cfg = {
        "custom_fields": [
            {"name": "extra_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "extra_disabled", "duckdb_type": "BIGINT", "enabled": False},
        ]
    }
    sql = get_ingest_columns_sql(cfg)
    assert "'extra_a': 'VARCHAR'" in sql and "extra_disabled" not in sql


def test_get_ingest_columns_sql_escapes_quote_in_field_metadata():
    cfg = {"custom_fields": [{"name": "hostile", "duckdb_type": "VARCHAR'; DROP TABLE", "enabled": True}]}
    sql = get_ingest_columns_sql(cfg)
    assert "VARCHAR''" in sql and "VARCHAR'; DROP" not in sql


def test_delete_objects_robust_returns_zero_for_empty_list():
    fake_s3 = MagicMock()
    assert _delete_objects_robust(fake_s3, "bkt", []) == 0
    fake_s3.delete_objects.assert_not_called()


def test_delete_objects_robust_uses_bulk_delete_for_happy_path():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {}
    assert _delete_objects_robust(fake_s3, "bkt", [f"k{i}" for i in range(20)]) == 20
    fake_s3.delete_objects.assert_called_once()
    payload = fake_s3.delete_objects.call_args.kwargs["Delete"]
    assert len(payload["Objects"]) == 20 and payload["Quiet"] is True


def test_delete_objects_robust_reports_exact_partial_failures():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {"Errors": [{"Key": "k2", "Code": "InternalError", "Message": "retry later"}]}
    assert _delete_objects_robust_with_failures(fake_s3, "bkt", ["k1", "k2", "k3"]) == (2, {"k2"})


def test_delete_objects_robust_batches_in_500_chunks():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {}
    assert _delete_objects_robust(fake_s3, "bkt", [f"k{i}" for i in range(1234)]) == 1234
    assert fake_s3.delete_objects.call_count == 3


def test_delete_objects_robust_short_circuits_on_access_denied_in_bulk():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {"Errors": [{"Code": "AccessDenied", "Message": "x"}]}
    assert _delete_objects_robust(fake_s3, "bkt", [f"k{i}" for i in range(10)]) == 0


def test_delete_objects_robust_falls_back_to_individual_on_bulk_exception():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("UnsupportedOperation")
    fake_s3.delete_object.return_value = {}
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3"]) == 3
    assert fake_s3.delete_object.call_count == 3


def test_delete_objects_robust_returns_zero_when_bulk_fails_with_access_denied():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("AccessDenied: not allowed")
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2"]) == 0
    fake_s3.delete_object.assert_not_called()


def test_delete_objects_robust_individual_fallback_stops_on_access_denied():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("bulk failed")
    call_count = {"n": 0}

    def _maybe_fail(*, Bucket, Key):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("AccessDenied")
        return {}

    fake_s3.delete_object.side_effect = _maybe_fail
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3", "k4"]) == 1
    assert call_count["n"] == 2


def test_delete_objects_robust_individual_fallback_continues_on_other_errors():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("bulk failed")
    call_count = {"n": 0}

    def _maybe_fail(*, Bucket, Key):
        call_count["n"] += 1
        if Key == "k2":
            raise RuntimeError("NoSuchKey: gone")
        return {}

    fake_s3.delete_object.side_effect = _maybe_fail
    assert _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3"]) == 2
    assert call_count["n"] == 3


@pytest.mark.parametrize(
    ("fname", "expected"),
    [
        ("2026-05-04T18-30-00.svc1234.gz", datetime(2026, 5, 4, 18, 30, 0, tzinfo=UTC)),
        ("2026-01-15T09-00-00.svc.gz", datetime(2026, 1, 15, 9, 0, 0, tzinfo=UTC)),
        ("2026-05-26T18:36:50.000-WFFKlnSyTWAMB3qwtVDD.log.gz", datetime(2026, 5, 26, 18, 36, 50, tzinfo=UTC)),
        ("manifest.json", None),
        ("readme.txt", None),
        (".DS_Store", None),
    ],
)
def test_parse_fastly_filename_dt(fname: str, expected: datetime | None):
    assert _parse_fastly_filename_dt(fname) == expected


@pytest.mark.parametrize(
    ("already", "lookback", "expected"),
    [
        (set(), 4, None),
        (
            {
                "s3://b/raw/2026-05-04/12/2026-05-04T12-00-00.svc.gz",
                "s3://b/raw/2026-05-04/15/2026-05-04T15-30-00.svc.gz",
            },
            4,
            "raw/2026-05-04/11/",
        ),
        ({"s3://b/raw/garbage-entry.gz", "s3://b/raw/another.json"}, 4, None),
        ({"s3://b/raw/2026-05-04/15/2026-05-04T15-00-00.svc.gz"}, 24, "raw/2026-05-03/15/"),
    ],
)
def test_compute_incremental_start_after(already: set[str], lookback: int, expected: str | None):
    assert _compute_incremental_start_after(already, lookback_hours=lookback) == expected


def _drain_ingest(gen):
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


@pytest.mark.parametrize(
    ("src", "needle"),
    [
        ({"name": "test-svc", "service_id": "svc-1", "access_level": "read_only", "bucket": "b"}, "read-only"),
        ({"name": "test-svc", "service_id": "svc-1", "bucket": ""}, "bucket"),
    ],
)
def test_ingest_yields_error_for_invalid_source(src: dict, needle: str):
    with patch("backend.core.ingest._ensure_source_registered"):
        events, _ = _drain_ingest(ingest.ingest(source=src))
    assert any(e.get("type") == "error" and needle in e["message"].lower() for e in events)


def test_ingest_short_circuits_to_done_when_no_new_files():
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = []
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator
    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value={"already1.gz", "already2.gz"}),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
    ):
        events, _ = _drain_ingest(ingest.ingest(source=src))
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None and done["new_files"] == 0 and done["skipped_files"] == 0


def test_ingest_yields_error_on_fos_list_failure():
    fake_s3 = MagicMock()
    fake_s3.get_paginator.side_effect = RuntimeError("S3 connection refused")
    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
    ):
        events, _ = _drain_ingest(ingest.ingest(source=src))
    error = next((e for e in events if e["type"] == "error"), None)
    assert error is not None and (
        "connection refused" in error["message"].lower() or "list" in error["message"].lower()
    )


def test_ingest_sets_force_download_on_mem_con():
    executed_sql: list[str] = []

    class _RecordingCon:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args, **kwargs):
            executed_sql.append(sql)
            return self._real.execute(sql, *args, **kwargs)

        def close(self):
            return self._real.close()

        def __getattr__(self, name):
            return getattr(self._real, name)

    def _fake_get_mem_con(_src):
        con = duckdb.connect(":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        return _RecordingCon(con)

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw/2026-05-20/00/2026-05-20T00-00-00.000.gz", "Size": 1024}]}
    ]
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator
    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_memory_connection", side_effect=_fake_get_mem_con),
        patch("backend.core.ingest._execute_query_with_retry", side_effect=RuntimeError("synthetic stop")),
    ):
        _drain_ingest(ingest.ingest(source=src))
    normalized = [" ".join(s.split()).lower() for s in executed_sql]
    assert any("set force_download = true" in s for s in normalized)


def _src(name: str, akid: str, *, cdn_secret: str = "") -> dict:
    return {
        "name": name,
        "service_id": name,
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "https://us-east-1.object.fastlystorage.app",
        "access_key_id": akid,
        "secret_access_key": "secret",
        "cdn_url": "https://cdn.example.test",
        "cdn_secret": cdn_secret,
    }


def _reset_cache() -> None:
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()


def test_same_creds_reuse_cached_client():
    _reset_cache()
    try:
        assert _ddb._get_fos_client(_src("svc-a", "KEY1")) is _ddb._get_fos_client(_src("svc-a", "KEY1"))
    finally:
        _reset_cache()


def test_rotated_key_misses_cache():
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-rot", "OLDKEY"))
        c_new = _ddb._get_fos_client(_src("svc-rot", "NEWKEY"))
        assert c_old is not c_new
        keys = {k[:2] for k in _ddb._fos_client_cache if k[0] == "svc-rot"}
        assert ("svc-rot", "OLDKEY") in keys and ("svc-rot", "NEWKEY") in keys
    finally:
        _reset_cache()


def test_rotated_cdn_secret_misses_cache():
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="OLDSECRET"))
        c_new = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="NEWSECRET"))
        assert c_old is not c_new
    finally:
        _reset_cache()


def test_clear_fos_client_drops_all_entries_for_name():
    _reset_cache()
    try:
        _ddb._get_fos_client(_src("svc-clear", "OLDKEY"))
        _ddb._get_fos_client(_src("svc-clear", "NEWKEY"))
        _ddb._get_fos_client(_src("other", "K"))
        _ddb.clear_fos_client("svc-clear")
        remaining = set(_ddb._fos_client_cache)
        assert not any(k[0] == "svc-clear" for k in remaining)
        assert any(k[:2] == ("other", "K") for k in remaining)
        _ddb.clear_fos_client({"name": "other"})
        assert not any(k[:2] == ("other", "K") for k in _ddb._fos_client_cache)
    finally:
        _reset_cache()


def test_clear_fos_client_is_noop_when_absent():
    _reset_cache()
    _ddb.clear_fos_client("never-built")
    _ddb.clear_fos_client({"name": "never-built"})


def test_deterministic_buffer_name_is_stable_and_distinct():
    name1 = ingest._deterministic_buffer_name(["s3://b/a.gz", "s3://b/b.gz"])
    name2 = ingest._deterministic_buffer_name(["s3://b/b.gz", "s3://b/a.gz"])
    assert name1 == name2 and name1.startswith("batch_") and name1.endswith(".parquet")
    assert len(name1) == len("batch_") + 16 + len(".parquet")
    assert ingest._deterministic_buffer_name(["s3://b/a.gz"]) != ingest._deterministic_buffer_name(["s3://b/b.gz"])


def test_record_list_overwrite_and_clear_in_flight():
    sid = "svc-inflight-roundtrip"
    rows = [("s3://b/a.gz", 100, 4096), ("s3://b/b.gz", 200, 8192)]
    metadata_db.record_in_flight(sid, "batch_abc.parquet", rows)
    assert metadata_db.list_in_flight(sid) == [("batch_abc.parquet", rows)]

    sid_u = "svc-inflight-upsert"
    metadata_db.record_in_flight(sid_u, "batch_xyz.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight(sid_u, "batch_xyz.parquet", [("a", 1, 10), ("b", 2, 20)])
    assert metadata_db.list_in_flight(sid_u) == [("batch_xyz.parquet", [("a", 1, 10), ("b", 2, 20)])]

    sid_c = "svc-inflight-clear"
    metadata_db.record_in_flight(sid_c, "batch_clr.parquet", [("a", 1, 10)])
    metadata_db.clear_in_flight(sid_c, "batch_clr.parquet")
    metadata_db.clear_in_flight(sid_c, "batch_clr.parquet")
    assert metadata_db.list_in_flight(sid_c) == []


def test_recover_in_flight_promotes_when_buffer_exists(tmp_path):
    sid = "svc-recover-promote"
    metadata_db.record_in_flight(sid, "batch_present.parquet", [("s3://b/x.gz", 50, 1000), ("s3://b/y.gz", 75, 2000)])
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_present.parquet").write_bytes(b"PAR1stub")
    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight({"name": sid})
    assert result == {"promoted": 1, "dropped": 0, "rows_recovered": 125}
    assert metadata_db.get_ingested_filenames(sid) == {"s3://b/x.gz", "s3://b/y.gz"}
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_drops_when_buffer_missing(tmp_path):
    sid = "svc-recover-drop"
    metadata_db.record_in_flight(sid, "batch_missing.parquet", [("s3://b/q.gz", 10, 100)])
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight({"name": sid})
    assert result["promoted"] == 0 and result["dropped"] == 1
    assert metadata_db.get_ingested_filenames(sid) == set() and metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_is_noop_when_no_pending(tmp_path):
    with patch("backend.core.iceberg._buffer_dir", return_value=str(tmp_path)):
        assert ingest._recover_in_flight({"name": "svc-recover-empty"}) == {
            "promoted": 0,
            "dropped": 0,
            "rows_recovered": 0,
        }


def test_recover_in_flight_handles_mixed_state(tmp_path):
    sid = "svc-recover-mixed"
    metadata_db.record_in_flight(sid, "batch_here.parquet", [("a.gz", 5, 50)])
    metadata_db.record_in_flight(sid, "batch_gone.parquet", [("b.gz", 7, 70)])
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_here.parquet").write_bytes(b"PAR1stub")
    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight({"name": sid})
    assert result == {"promoted": 1, "dropped": 1, "rows_recovered": 5}
    assert metadata_db.get_ingested_filenames(sid) == {"a.gz"} and metadata_db.list_in_flight(sid) == []


def test_list_in_flight_is_per_service():
    metadata_db.record_in_flight("svc-inflight-a", "batch_a.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight("svc-inflight-b", "batch_b.parquet", [("b", 2, 20)])
    a_pending = metadata_db.list_in_flight("svc-inflight-a")
    b_pending = metadata_db.list_in_flight("svc-inflight-b")
    assert len(a_pending) == 1 and a_pending[0][0] == "batch_a.parquet"
    assert len(b_pending) == 1 and b_pending[0][0] == "batch_b.parquet"


def test_recover_in_flight_survives_buffer_dir_missing(tmp_path):
    sid = "svc-recover-no-dir"
    metadata_db.record_in_flight(sid, "batch_nodir.parquet", [("a.gz", 1, 1)])
    missing_dir = str(tmp_path / "does_not_exist" / "buffer")
    assert not os.path.exists(missing_dir)
    with patch("backend.core.iceberg._buffer_dir", return_value=missing_dir):
        result = ingest._recover_in_flight({"name": sid})
    assert result["dropped"] == 1 and result["promoted"] == 0


SERVICE_ID = "test-ingest-task-wrappers"


def _depths(depth: int, ok: bool = True):
    return patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": depth}, ok))


@pytest.mark.parametrize(
    ("task_attr", "target_fn", "other_fn", "keys"),
    [
        (
            "convert_batch_files",
            "convert_batch_objects",
            "convert_batch_rum_objects",
            ["raw/a.json.gz", "raw/b.json.gz", "raw/c.json.gz"],
        ),
        (
            "convert_batch_rum_files",
            "convert_batch_rum_objects",
            "convert_batch_objects",
            ["raw/rum/a.json.gz", "raw/rum/b.json.gz"],
        ),
    ],
)
def test_convert_batch_task_routes_to_correct_converter(task_attr: str, target_fn: str, other_fn: str, keys: list[str]):
    task = getattr(ingest, task_attr)
    with (
        patch.object(ingest, target_fn, return_value={"committed": len(keys)}) as mock_target,
        patch.object(ingest, other_fn) as mock_other,
    ):
        assert task.apply(args=(SERVICE_ID, keys)).get() == {"committed": len(keys)}
    mock_target.assert_called_once()
    assert mock_target.call_args.args[:2] == (SERVICE_ID, keys) and mock_target.call_args.args[2]
    mock_other.assert_not_called()


def test_every_ledger_task_is_registered_under_its_module_path():
    for name, task in {
        "backend.core.ingest.convert_batch_files": ingest.convert_batch_files,
        "backend.core.ingest.convert_batch_rum_files": ingest.convert_batch_rum_files,
    }.items():
        assert task.name == name and name in ingest.app.tasks and ingest.app.tasks[name].name == name


def test_celery_ledger_claim_semantics():
    service_id, object_key = "test-celery-svc", "raw/2026/08/27/10/05/logs.json.gz"
    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by) VALUES (?, ?, 'claimed', 'worker-1')",
        (service_id, object_key),
    )
    con.commit()
    with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test"}):
        ingest.convert_batch_files.push_request(id="worker-2")
        try:
            ingest.convert_batch_files(service_id, [object_key])
        finally:
            ingest.convert_batch_files.pop_request()
    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    )
    row = cur.fetchone()
    assert row["status"] == "claimed" and row["claimed_by"] == "worker-1"


@pytest.mark.parametrize("label", ["", "RUM "])
def test_dispatches_reclaimed_then_stuck_in_batches(label: str) -> None:
    dispatch = MagicMock()
    n = ingest.LEDGER_CONVERT_BATCH_SIZE
    reclaimed, stuck = [f"r{i}" for i in range(3)], [f"s{i}" for i in range(n)]
    with _depths(0):
        assert ingest._redispatch_pending("svc", reclaimed, stuck, dispatch, label) == (n + 3, True)
    sent = [c.args for c in dispatch.call_args_list]
    assert all(a[0] == "svc" for a in sent)
    assert [k for a in sent for k in a[1]] == reclaimed + stuck
    assert [len(a[1]) for a in sent] == [n, 3]


def test_redispatch_pending_skip_conditions() -> None:
    dispatch = MagicMock()
    with _depths(10):
        assert ingest._redispatch_pending("svc", [], ["a"], dispatch, "") == (0, True)
    with _depths(0, ok=False):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "RUM ") == (0, False)
    with patch("backend.celery_status.celery_queue_depths", side_effect=RuntimeError("boom")):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "") == (0, False)
    assert ingest._redispatch_pending("svc", [], [], dispatch, "") == (0, True)
    dispatch.assert_not_called()


@pytest.mark.parametrize(
    ("sweep_name", "delay_name", "prefix"),
    [
        ("sweep_ledger_once", "convert_batch_files", "raw/2026/08/27"),
        ("sweep_rum_ledger_once", "convert_batch_rum_files", "raw/rum/2026/08/27"),
    ],
)
def test_sweep_dispatches_each_pending_key_once_with_5000_rows(sweep_name: str, delay_name: str, prefix: str) -> None:
    service_id = "test-celery-svc"
    con, cur = clear_ledger(service_id)
    now = time.time()
    stale_claim, old = now - ingest.LEDGER_RECLAIM_AFTER_S - 60, now - 3600
    rows = [
        (service_id, f"{prefix}/claimed_{i:05d}.json.gz", "claimed", "dead", stale_claim, old) for i in range(2500)
    ] + [(service_id, f"{prefix}/stuck_{i:05d}.json.gz", "discovered", None, None, old) for i in range(2500)]
    cur.executemany(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.commit()

    def _no_files(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "b"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_no_files),
        patch("backend.core.ingest.discover_prefix", return_value=0),
        patch("backend.core.ingest.discover_rum_prefix", return_value=0),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
        patch(f"backend.core.ingest.{delay_name}.delay") as delay,
    ):
        summary = getattr(ingest, sweep_name)(service_id)

    keys = [k for c in delay.call_args_list for k in c.args[1]]
    assert summary["reclaimed"] == 2500 and summary["redispatched"] == 5000
    assert len(keys) == len(set(keys)) == 5000
    assert delay.call_count == -(-5000 // ingest.LEDGER_CONVERT_BATCH_SIZE)


_make_source = partial(
    make_source,
    kind="cloud",
    cdn_url="",
    cdn_secret="",
    cdn_service_id="",
    logging_service_id="test",
    duckdb_path=":memory:",
    log_period=60,
    provisioning={},
)


def _drain(gen) -> list[dict]:
    return list(gen)


class TestIngestReadOnlyGuard:
    def test_read_only_yields_error_immediately(self):
        events = _drain(ingest.ingest(source=_make_source(access_level="read_only")))
        assert len(events) == 1 and events[0]["type"] == "error" and "read-only" in events[0]["message"].lower()

    def test_read_write_does_not_error_on_access_level(self):
        with patch("backend.core.ingest._ensure_source_registered", side_effect=ValueError("stop")):
            events, _ = _drain_ingest(ingest.ingest(source=_make_source(access_level="read_write")))
        assert not any(e.get("type") == "error" and "read-only" in e.get("message", "").lower() for e in events)


class TestIngestStartTimeExec:
    def test_start_time_exec_is_float(self):
        call_count = {"n": 0}
        real_start = time.time()

        def fake_time():
            call_count["n"] += 1
            return real_start if call_count["n"] == 1 else real_start + 999.0

        fake_file = "raw/2026-01-01/00/2026-01-01T00-00-00.svc.gz"
        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._get_fos_client") as mock_fos,
            patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
            patch("backend.core.metadata.insert_ingested_files"),
            patch("time.time", side_effect=fake_time),
        ):
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = iter([{"Contents": [{"Key": fake_file, "Size": 100}]}])
            mock_fos.return_value.get_paginator.return_value = mock_paginator
            events = list(ingest.ingest(source=_make_source(), max_seconds=1, start_time="2026-01-01T00:00:00Z"))
        assert not any("unsupported operand" in e.get("message", "") for e in events if e.get("type") == "error")


class TestIngestMaxSeconds:
    def test_expired_listing_budget_still_attempts_first_batch(self):
        fake_file = "s3://bucket/raw/request/year=2026/month=01/day=01/hour=00/minute=00/test.log.gz"

        def fake_list_fos_files(**_kwargs):
            yield {"type": "status", "message": "Discovering"}
            return {
                "new_files": [fake_file],
                "file_sizes": {fake_file: 100},
                "skipped_already": 0,
                "stranded_already": [],
            }

        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._recover_in_flight", return_value={"promoted": 0, "dropped": 0}),
            patch("backend.core.ingest._get_fos_client"),
            patch("backend.core.ingest.metadata_db.get_ingested_filenames", return_value=set()),
            patch("backend.core.ingest.list_fos_files", side_effect=fake_list_fos_files),
            patch("backend.core.duckdb.get_memory_connection", return_value=MagicMock()),
            patch("backend.core.ingest._download_chunk_to_local", return_value=({}, {})) as download,
            patch("backend.config.load_config", return_value={}),
            patch("backend.core.ingest.time.time", side_effect=[1_000.0, 1_999.0, 1_999.0, 1_999.0]),
        ):
            events = _drain(ingest.ingest(source=_make_source(), max_seconds=240, incremental_only=True))
        download.assert_called_once()
        assert any("Processing 1 files in batch 1/1" in event.get("message", "") for event in events)

    def test_max_seconds_zero_and_type_annotations(self):
        events = _drain(ingest.ingest(source=_make_source(access_level="read_only"), max_seconds=0))
        assert events[0]["type"] == "error"
        sig = inspect.signature(ingest.ingest)
        max_s = str(sig.parameters["max_seconds"].annotation)
        start_t = str(sig.parameters["start_time"].annotation)
        assert "int" in max_s and "None" in max_s
        assert "str" in start_t and "None" in start_t and "float" not in start_t


def _run_workers(targets):
    threads = [threading.Thread(target=t, args=a) for t, a in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive(), "worker thread hung past 30s"


def test_concurrent_ingest_calls_one_wins_one_is_no_op(ingest_local_env, monkeypatch):  # noqa: F811
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    service_id = src["name"]
    key = "raw/2026-06-10/08/2026-06-10T08-00-00.solo.gz"
    _seed_local(log_dir, key, "2026-06-10T08:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])
    barrier, results, lock = threading.Barrier(2), {}, threading.Lock()

    def worker(label: str) -> None:
        barrier.wait()
        outcome: dict = {"label": label}
        try:
            run_id = metadata_db.start_cron_run(service_id, "sync")
            outcome["run_id"], outcome["winner"] = run_id, True
            try:
                events = _drain(ingest.ingest(source=src))
                done = next((e for e in events if e["type"] == "done"), None)
                outcome["rows_inserted"] = done.get("rows_inserted") if done else None
            finally:
                try:
                    metadata_db.log_cron_run(service_id, "sync", 0.0, "success", run_id=run_id)
                except Exception:
                    pass
        except RuntimeError as exc:
            outcome["winner"], outcome["error"] = False, str(exc)
        with lock:
            results[label] = outcome

    _run_workers([(worker, ("a",)), (worker, ("b",))])
    winners = [r for r in results.values() if r.get("winner")]
    losers = [r for r in results.values() if not r.get("winner")]
    assert len(winners) == 1 and len(losers) == 1
    assert "already running" in losers[0]["error"].lower() and winners[0]["rows_inserted"] == 1
    assert len([n for n in metadata_db.get_ingested_filenames(service_id) if "solo.gz" in n]) == 1


def test_concurrent_ingest_on_different_sources_both_succeed(ingest_local_env, monkeypatch):  # noqa: F811
    log_dir = ingest_local_env["log_dir"]
    src_a = dict(ingest_local_env["src"])
    src_b = {**src_a, "name": "test_service_b", "service_id": "test-service-b"}
    bucket_prefix, file_prefix = f"s3://{src_a['bucket']}/", "file://" + str(log_dir.absolute()) + "/"

    monkeypatch.setattr(
        _ddb,
        "get_memory_connection",
        lambda _src: _S3RewritingConn(duckdb.connect(":memory:"), bucket_prefix, file_prefix),
    )
    monkeypatch.setattr(
        _ddb, "_cache_dir", lambda s: str(log_dir.parent / "cache" / str(s.get("service_id") or s["name"]))
    )

    key_a = "raw/2026-06-11/09/2026-06-11T09-00-00.a.gz"
    key_b = "raw/2026-06-11/09/2026-06-11T09-00-00.b.gz"
    _seed_local(log_dir, key_a, "2026-06-11T09:00:00Z")
    _seed_local(log_dir, key_b, "2026-06-11T09:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key_a, key_b])

    barrier, results, lock = threading.Barrier(2), {}, threading.Lock()

    def worker(label: str, src: dict) -> None:
        barrier.wait()
        outcome: dict = {"label": label, "service_id": src["name"]}
        try:
            run_id = metadata_db.start_cron_run(src["name"], "sync")
            outcome["run_id"], outcome["winner"] = run_id, True
            events = _drain(ingest.ingest(source=src))
            done = next((e for e in events if e["type"] == "done"), None)
            outcome["rows_inserted"] = done.get("rows_inserted") if done else None
            try:
                metadata_db.log_cron_run(src["name"], "sync", 0.0, "success", run_id=run_id)
            except Exception:
                pass
        except RuntimeError as exc:
            outcome["winner"], outcome["error"] = False, str(exc)
        with lock:
            results[label] = outcome

    _run_workers([(worker, ("a", src_a)), (worker, ("b", src_b))])
    for r in results.values():
        assert r.get("winner") is True and r["rows_inserted"] >= 1
    assert metadata_db.get_ingested_filenames(src_a["name"]) and metadata_db.get_ingested_filenames(src_b["name"])


_buffer_names = st.text(alphabet=string.ascii_lowercase, min_size=3, max_size=8).map(lambda s: f"batch_{s}.parquet")
_file_names = st.text(alphabet=string.ascii_lowercase + string.digits, min_size=3, max_size=8).map(
    lambda s: f"raw/2026-06-17/00/{s}.gz"
)
_file_manifest = st.tuples(
    _file_names,
    st.integers(min_value=0, max_value=10_000),
    st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000)),
)
_file_manifests = st.lists(_file_manifest, min_size=1, max_size=4, unique_by=lambda t: t[0])


class IngestMetadataStateMachine(RuleBasedStateMachine):
    buffers = Bundle("buffers")

    @initialize()
    def setup(self):
        self.service_id = "svc-hypothesis-ingest"
        con = metadata_db.get_con(self.service_id)
        for tbl in ("ingest_in_flight", "ingested_files", "ingested_files_summary"):
            con.execute(f"DELETE FROM {tbl} WHERE source_name = ?", (self.service_id,))
        con.commit()
        metadata_db.teardown(self.service_id)
        self.shadow_in_flight: dict[str, list[tuple[str, int, int | None]]] = {}
        self.shadow_ingested: set[str] = set()

    @rule(target=buffers, buffer=_buffer_names, files=_file_manifests)
    def record_in_flight(self, buffer, files):
        files_t = [tuple(f) for f in files]
        metadata_db.record_in_flight(self.service_id, buffer, files_t)
        self.shadow_in_flight[buffer] = files_t
        return buffer

    @rule(buffer=consumes(buffers))
    def promote_and_clear(self, buffer):
        files = self.shadow_in_flight.pop(buffer, None)
        if files is None:
            return
        metadata_db.insert_ingested_files(self.service_id, files)
        metadata_db.clear_in_flight(self.service_id, buffer)
        self.shadow_ingested.update(fname for (fname, _, _) in files)

    @rule(buffer=consumes(buffers))
    def crash_drop(self, buffer):
        self.shadow_in_flight.pop(buffer, None)
        metadata_db.clear_in_flight(self.service_id, buffer)

    @rule(buffer=_buffer_names)
    def idempotent_clear(self, buffer):
        if buffer not in self.shadow_in_flight:
            metadata_db.clear_in_flight(self.service_id, buffer)

    @invariant()
    def in_flight_matches_shadow(self):
        db_state = {buffer: list(files) for buffer, files in metadata_db.list_in_flight(self.service_id)}
        assert set(db_state.keys()) == set(self.shadow_in_flight.keys())
        for buffer, shadow_files in self.shadow_in_flight.items():
            assert db_state[buffer] == shadow_files

    @invariant()
    def ingested_matches_shadow(self):
        assert metadata_db.get_ingested_filenames(self.service_id) == self.shadow_ingested

    @invariant()
    def rollup_count_matches_shadow(self):
        con = metadata_base.get_con(self.service_id)
        row = con.execute(
            "SELECT file_count FROM ingested_files_summary WHERE source_name = ?", (self.service_id,)
        ).fetchone()
        assert (row["file_count"] if row else 0) == len(self.shadow_ingested)


TestIngestMetadataStateMachine = IngestMetadataStateMachine.TestCase
TestIngestMetadataStateMachine.settings = settings(
    max_examples=20,
    deadline=None,
    stateful_step_count=15,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
