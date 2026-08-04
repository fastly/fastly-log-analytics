"""Tests for backend.core.reset.reset_service_logs — the Project Log Reset
("Delete Data") generator.

Focuses on the safety-critical guarantees called out in
local-docs/log_reset_design_plan.md: config preservation vs. operational
wipe, the per-service lock exclusion, the cron_busy guard, the
try/finally scheduler-resume-and-lock-release resilience under a mid-run
failure, and the iceberg/meta/ + raw/ cloud-purge scoping. Iceberg catalog
internals (init_iceberg_table / update_iceberg_view) are covered by their
own test modules, so they're stubbed here to keep this suite focused on
reset.py's own orchestration.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from backend import config as svcconfig
from backend.core import reset as reset_mod
from backend.core.metadata.base import get_con
from tests.conftest import MOCK_SERVICE_ID


def _save_config(**cron_sync_overrides):
    cron_sync = {"enabled": True, **cron_sync_overrides}
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
            "provisioning": {"cron_sync": cron_sync},
        },
    )


def _seed_metadata_rows():
    """Insert one row into every WIPE table and every PRESERVE table."""
    con = get_con(MOCK_SERVICE_ID)
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) "
        "VALUES ('a.gz', ?, 10, 100, '2026-01-01')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO ingested_files_summary (source_name, file_count, total_rows, total_bytes) VALUES (?, 1, 10, 100)",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json) VALUES ('buf1', ?, '[]')",
        (MOCK_SERVICE_ID,),
    )
    con.execute("INSERT INTO committed_buffers (buffer_filename) VALUES ('buf1')")
    con.execute("INSERT INTO local_compacted_files (file_name) VALUES ('a.gz')")
    con.execute(
        "INSERT INTO quarantined_files (file_name, source_name, fos_key, error_key, meta_key) "
        "VALUES ('bad.gz', ?, 'raw/bad.gz', 'errors/bad.bad.jsonl', 'errors/bad.meta.json')",
        (MOCK_SERVICE_ID,),
    )

    con.execute("INSERT INTO sources (name, config, table_name) VALUES (?, '{}', 'logs')", (MOCK_SERVICE_ID,))
    con.execute(
        "INSERT INTO views (id, service_id, name, filters_json) VALUES ('v1', ?, 'My View', '{}')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO alerts (id, service_id, name, metric, operator, threshold, window_min) "
        "VALUES ('al1', ?, 'My Alert', 'error_rate', '>', 5.0, 10.0)",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO scoring_labels (id, service_id, sid, label) VALUES ('sl1', ?, 'session1', 'good')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO scoring_audit (service_id, action, actor) VALUES (?, 'relabel', 'ui')",
        (MOCK_SERVICE_ID,),
    )
    con.execute("INSERT INTO asn_names (asn, name) VALUES (12345, 'Test ASN')")
    con.commit()


def _table_count(table: str) -> int:
    con = get_con(MOCK_SERVICE_ID)
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


@pytest.fixture(autouse=True)
def _stub_iceberg_catalog(monkeypatch):
    """Stub the catalog-rebuild steps — exercised by test_iceberg.py, not here."""
    monkeypatch.setattr("backend.core.iceberg._core.init_iceberg_table", MagicMock())
    monkeypatch.setattr("backend.core.iceberg.view.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.get_connection", MagicMock())


@pytest.fixture
def fake_scheduler():
    """A stand-in for the ``reload_scheduler`` callback the router/CLI inject
    (real callers pass ``lambda: get_scheduler().reload()`` — see the
    layering note on ``reset_service_logs``)."""
    return MagicMock()


def test_wipes_operational_preserves_config_and_meta(s3_mock, fos_source, fake_scheduler):
    _save_config()
    _seed_metadata_rows()
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/default/logs/data/foo.parquet", Body=b"x")
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/meta/admin_state.json", Body=b"{}")
    s3_mock.put_object(Bucket="test-bucket", Key="errors/bad.bad.jsonl", Body=b"bad")
    s3_mock.put_object(Bucket="test-bucket", Key="raw/2026/01/01/00/log.gz", Body=b"raw")

    events = list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler))

    assert events[-1]["type"] == "done"

    for table in (
        "ingested_files",
        "ingested_files_summary",
        "ingest_in_flight",
        "committed_buffers",
        "local_compacted_files",
        "quarantined_files",
    ):
        assert _table_count(table) == 0, f"{table} should be wiped"

    for table in ("sources", "views", "alerts", "scoring_labels", "scoring_audit", "asn_names"):
        assert _table_count(table) == 1, f"{table} should be preserved"

    # record_audit("logs_reset") is written on top of the pre-seeded row.
    assert _table_count("audit_logs") == 1

    keys = {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}
    assert "iceberg/default/logs/data/foo.parquet" not in keys
    assert "iceberg/meta/admin_state.json" in keys, "iceberg/meta/ must survive the purge"
    assert "errors/bad.bad.jsonl" not in keys
    assert "raw/2026/01/01/00/log.gz" in keys, "raw/ must be left alone when delete_raw_logs=False (default)"

    # Scheduler paused then resumed, and the config flag restored.
    assert fake_scheduler.call_count >= 2
    cfg = svcconfig.load_config(MOCK_SERVICE_ID)
    assert cfg["provisioning"]["cron_sync"]["enabled"] is True


def test_delete_raw_logs_true_purges_raw_prefix(s3_mock, fos_source, fake_scheduler):
    _save_config()
    s3_mock.put_object(Bucket="test-bucket", Key="raw/2026/01/01/00/log.gz", Body=b"raw")

    list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, delete_raw_logs=True, reload_scheduler=fake_scheduler))

    keys = {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}
    assert "raw/2026/01/01/00/log.gz" not in keys


def test_cron_busy_blocks_reset(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    monkeypatch.setattr("backend.core.metadata.cron_busy", lambda service_id: True)

    gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
    with pytest.raises(RuntimeError, match="Active sync or commit"):
        next(gen)

    # The lock must not be left held after the guard raises.
    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1)
    lock.release()


def test_concurrent_reset_is_excluded_by_service_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    """RLock is reentrant per-thread, so exclusion only shows up against a
    genuinely different thread holding the lock — the same-thread case
    would silently reacquire and defeat the point of this test."""
    import threading

    _save_config()
    monkeypatch.setattr(reset_mod, "_LOCK_TIMEOUT_S", 0.1)

    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    held = threading.Event()
    release = threading.Event()

    def _hold_lock():
        lock.acquire()
        held.set()
        release.wait(timeout=5)
        lock.release()

    holder = threading.Thread(target=_hold_lock, daemon=True)
    holder.start()
    held.wait(timeout=5)
    try:
        gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
        with pytest.raises(RuntimeError, match="Another operation is in progress"):
            next(gen)
    finally:
        release.set()
        holder.join(timeout=5)


def test_error_mid_reset_still_resumes_scheduler_and_releases_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    # _purge_prefix only calls the delete_fn for pages that actually list
    # objects — seed one so the mocked failure is reached (an empty
    # bucket would otherwise never invoke it).
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/default/logs/data/foo.parquet", Body=b"x")
    monkeypatch.setattr(
        "backend.core.ingest._delete_objects_robust",
        MagicMock(side_effect=RuntimeError("FOS unreachable")),
    )

    gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
    with pytest.raises(RuntimeError, match="FOS unreachable"):
        for _ in gen:
            pass

    cfg = svcconfig.load_config(MOCK_SERVICE_ID)
    assert cfg["provisioning"]["cron_sync"]["enabled"] is True, "must resume the paused sync even on failure"
    assert fake_scheduler.call_count >= 2

    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1), "lock must be released even when the reset raises"
    lock.release()


def test_no_service_raises_value_error(fake_scheduler):
    with pytest.raises(ValueError, match="No service found"):
        next(reset_mod.reset_service_logs("does-not-exist"))


def test_purge_prefix_reports_progress_per_page_and_returns_total():
    """A large prefix (thousands of Iceberg data files) should show live
    progress per listed page instead of going silent until it's all done —
    the exact gap reported live during a real reset of a 100k+-file
    service. Uses a fake paginator so this doesn't depend on moto's
    default 1000-key page size."""

    class _FakePaginator:
        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": "iceberg/a.parquet"}, {"Key": "iceberg/b.parquet"}]}
            yield {"Contents": [{"Key": "iceberg/c.parquet"}]}

    class _FakeFos:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return _FakePaginator()

    delete_calls = []

    def _fake_delete(fos_client, bucket, keys):
        delete_calls.append(list(keys))
        return len(keys)

    gen = reset_mod._purge_prefix(_FakeFos(), "bucket", "iceberg/", _fake_delete, label="thing(s)")
    result = _drain(gen)

    assert result["return"] == 3
    assert delete_calls == [["iceberg/a.parquet", "iceberg/b.parquet"], ["iceberg/c.parquet"]]
    # One status event per non-empty page, each showing the running total.
    assert [e["message"] for e in result["yielded"]] == [
        "Deleted 2 thing(s) so far...",
        "Deleted 3 thing(s) so far...",
    ]


def test_purge_prefix_excludes_prefix_and_skips_empty_pages():
    class _FakePaginator:
        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": "iceberg/meta/admin_state.json"}]}  # excluded, page has no deletable keys
            yield {"Contents": []}
            yield {"Contents": [{"Key": "iceberg/data/x.parquet"}]}

    class _FakeFos:
        def get_paginator(self, name):
            return _FakePaginator()

    def _fake_delete(fos_client, bucket, keys):
        return len(keys)

    gen = reset_mod._purge_prefix(
        _FakeFos(), "bucket", "iceberg/", _fake_delete, exclude_prefix="iceberg/meta/", label="thing(s)"
    )
    result = _drain(gen)
    assert result["return"] == 1
    assert [e["message"] for e in result["yielded"]] == ["Deleted 1 thing(s) so far..."]


def _drain(gen):
    yielded = []
    try:
        while True:
            yielded.append(next(gen))
    except StopIteration as stop:
        return {"yielded": yielded, "return": stop.value}


def test_remove_tree_with_progress_reports_batches_and_removes_everything(tmp_path):
    for i in range(5):
        (tmp_path / f"f{i}.parquet").write_bytes(b"x")

    gen = reset_mod._remove_tree_with_progress(str(tmp_path), batch_size=2)
    result = _drain(gen)

    assert result["return"] == 5
    # One progress event every 2 files removed (5 files -> events at 2 and 4).
    assert [e["message"] for e in result["yielded"]] == [
        "Removed 2 local cache files so far...",
        "Removed 4 local cache files so far...",
    ]
    assert not os.path.exists(tmp_path)
