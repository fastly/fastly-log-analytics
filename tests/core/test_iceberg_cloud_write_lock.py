"""Tests for the per-service lock guarding commit_buffer / optimize_table /
run_cloud_maintenance against backend.core.reset.reset_service_logs.

Root cause of a production incident: reset_service_logs() checks
metadata_db.cron_busy() exactly once, then holds the per-service lock
(_get_service_lock) for its whole run while it purges iceberg/ from FOS.
But commit_buffer/optimize_table/run_cloud_maintenance never acquired that
same lock, so a commit already past the one-time cron_busy() check could
append a new snapshot while the reset's purge deleted the manifest files it
depended on — corrupting even the freshly-recreated table. These tests
pin that the three cloud writers now serialize against the same lock a
reset holds, mirroring test_concurrent_reset_is_excluded_by_service_lock in
test_reset.py (which pins reset's own side of the exclusion).
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from backend.core.iceberg import buffer as buffer_mod
from backend.core.iceberg.view import _get_service_lock


@pytest.fixture
def lock_held_by_other_thread(fos_source):
    """Simulate reset_service_logs holding the per-service lock."""
    lock = _get_service_lock(fos_source["name"])
    held = threading.Event()
    release = threading.Event()

    def _hold():
        lock.acquire()
        held.set()
        release.wait(timeout=5)
        lock.release()

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    held.wait(timeout=5)
    yield
    release.set()
    holder.join(timeout=5)


def test_commit_buffer_skips_cleanly_when_lock_held(monkeypatch, fos_source, lock_held_by_other_thread):
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.1)
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)

    result = buffer_mod.commit_buffer(fos_source)

    impl.assert_not_called()
    assert result == {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}


def test_optimize_table_skips_cleanly_when_lock_held(monkeypatch, fos_source, lock_held_by_other_thread):
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.1)
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_optimize_table_impl", impl)

    result = buffer_mod.optimize_table(fos_source)

    impl.assert_not_called()
    assert result["files_rewritten"] == 0
    assert "busy" in result["error"]


def test_run_cloud_maintenance_skips_cleanly_when_lock_held(monkeypatch, fos_source, lock_held_by_other_thread):
    monkeypatch.setattr(buffer_mod, "_CLOUD_WRITE_LOCK_TIMEOUT_S", 0.1)
    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_run_cloud_maintenance_impl", impl)

    result = buffer_mod.run_cloud_maintenance(fos_source)

    impl.assert_not_called()
    assert "busy" in result["error"]


def test_commit_buffer_calls_impl_when_lock_free(monkeypatch, fos_source):
    """The wrapper must not change behavior on the normal, uncontended path."""
    impl = MagicMock(
        return_value={"files_committed": 3, "rows_committed": 30, "snapshot_id": 1, "quarantined_files": 0}
    )
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)

    result = buffer_mod.commit_buffer(fos_source)

    impl.assert_called_once_with(fos_source, None, table_name="logs")
    assert result["files_committed"] == 3

    # Lock must be released afterward — a later contender shouldn't block.
    lock = _get_service_lock(fos_source["name"])
    assert lock.acquire(timeout=0.1)
    lock.release()


def test_reset_excludes_a_concurrent_commit_attempt(monkeypatch, s3_mock, fos_source):
    """Integration-shaped version of the actual incident: while
    reset_service_logs holds the lock, a commit_buffer call racing it must
    be excluded rather than proceeding to touch FOS."""
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core import reset as reset_mod

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

    reached_middle = threading.Event()
    release_reset = threading.Event()

    def _slow_delete(*args, **kwargs):
        reached_middle.set()
        release_reset.wait(timeout=5)
        return 0

    monkeypatch.setattr("backend.core.ingest._delete_objects_robust", _slow_delete)
    s3_mock.put_object(Bucket=fos_source["bucket"], Key="iceberg/default/logs/data/foo.parquet", Body=b"x")

    impl = MagicMock()
    monkeypatch.setattr(buffer_mod, "_commit_buffer_impl", impl)

    def _run_reset():
        list(reset_mod.reset_service_logs(fos_source["service_id"], reload_scheduler=MagicMock()))

    reset_thread = threading.Thread(target=_run_reset, daemon=True)
    reset_thread.start()
    try:
        assert reached_middle.wait(timeout=5), "reset never reached the FOS purge"
        # Resolve the source the same way _run_log_ingest does in production
        # (get_source_for_service), NOT the raw fos_source fixture — its
        # "name" field doesn't equal its "service_id" the way a real
        # config-derived source's does, which would silently lock on a
        # different key than reset_service_logs does.
        real_source = _db.get_source_for_service(fos_source["service_id"])
        result = buffer_mod.commit_buffer(real_source)
        impl.assert_not_called()
        assert result["files_committed"] == 0
    finally:
        release_reset.set()
        reset_thread.join(timeout=5)
